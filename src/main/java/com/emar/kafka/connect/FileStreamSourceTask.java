package com.emar.kafka.connect;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.emar.kafka.interceptor.Scheme;
import com.emar.kafka.offset.OffsetValue;
import com.emar.kafka.utils.*;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDateTime;
import java.util.*;

/**
 * FileStreamSourceTask reads from stdin or a file.
 */
public class FileStreamSourceTask extends SourceTask {
    private static final Logger LOG = LoggerFactory.getLogger(FileStreamSourceTask.class);
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;
    private static final String CURRENT = "current";
    private static final String FILES = "file->position";
    private static final String INODES = "inode->file";
    private static final int DEFAULT_MAX_RM_INTERVAL_HOURS = 24;

    private static final int _1M = 1024 * 1024;
    private static final int _2M = 2 * _1M;
    private static final int _5M = 5 * _1M;
    private static final int _10M = 10 * _1M;
    private ByteBuffer buffer = null;

    private String path;
    private int index;
    private int checkCounter;
    private DirectoryStream.Filter<Path> filter;
    private boolean isRecursive;
    private int rmIntervalHours;
    private LocalDateTime start;
    private LocalDateTime lastModifyTime;
    private String filename;
    private long position;
    private FileChannel channel;
    private ArrayList<SourceRecord> records;
    private String topic = null;
    private Scheme scheme = null;

    private Map<String, OffsetValue> offset;
    private JSONObject filePositions;
    private JSONObject fileKeys;
    private boolean transferInodes;

    private String partitionKey;
    private String partitionValue;

    private long logOffsetTime = System.currentTimeMillis();
    private long checkFilesTime = System.currentTimeMillis();

    @Override
    public String version() {
        return new FileStreamSource().version();
    }

    @Override
    public void start(Map<String, String> props) {

        buffer = ByteBuffer.allocate(_1M);
        fileKeys = new JSONObject();
        filePositions = new JSONObject();
        transferInodes = false;

        path = props.get(FileStreamSource.PATH_CONFIG);
        if (path.endsWith("/")) {
            index = path.length();
        } else {
            index = path.length() + 1;
        }
        String filePrefix = props.get(FileStreamSource.FILE_PREFIX_CONFIG);
        String fileSuffix = props.get(FileStreamSource.FILE_SUFFIX_CONFIG);
        filePrefix = StringUtils.isBlank(filePrefix) ? "" : filePrefix;
        fileSuffix = StringUtils.isBlank(fileSuffix) ? "" : fileSuffix;
        String fileRegex = filePrefix + "*" + fileSuffix;
        final PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + fileRegex);

        filter = entry -> matcher.matches(entry.getFileName()) &&
                (boolean) Files.getAttribute(entry, "basic:isRegularFile");

        topic = props.get(FileStreamSource.TOPIC_CONFIG);
        if (topic == null) {
            throw new ConnectException("FileStreamSourceTask config missing topic setting");
        }
        partitionKey = "fileType: " + this.path + File.separator + fileRegex;
        partitionValue = "topic: " + topic;
        String startTime = props.get(FileStreamSource.START_TIME);

        String schemeClass = props.get(FileStreamSource.INTERCEPTOR_SCHEME);
        scheme = ConfigUtil.getInterceptorClass(schemeClass);

        isRecursive = Boolean.parseBoolean(props.get(FileStreamSource.IS_RECURSIVE_DIR));

        boolean ignoreOffset = Boolean.parseBoolean(props.get(FileStreamSource.IGNORE_OFFSET));
        if (ignoreOffset) {
            LOG.warn("ignore.offset 为 true, 将忽略之前的 offset，以 start.time 为首要参考对象");
            start = DateUtils.getStart(startTime);
            initOffset();
        } else {
            LOG.warn("ignore.offset 为 false（默认为 false）, 将以之前的 offset 为首要参考对象");
            Map<String, Object> storeOffset = context.offsetStorageReader().offset(offsetKey());
            if (storeOffset == null) {
                LOG.warn("Local stored offset is null! 以 start.time 为参考对象");
                start = DateUtils.getStart(startTime);
                initOffset();
            } else {
                storeOffsets(storeOffset);
                setupCheckOffset();
            }
        }
        LOG.info("采集的日志：path={}; fileType={}; topic={}; start.time={}", path, fileRegex, topic, start);
        LOG.info("current offset:{}", offset);
        try {
            changeStreamTo(0);
        } catch (IOException e) {
            LOG.error("Couldn't open stream:{} for FileStreamSourceTask!",
                    path + File.separator + filename);
            System.exit(1);
        }
        rmIntervalHours = StringUtils.getIntValue(props.get(FileStreamSource.RM_HISTORY_FILES_INTERVAL_HOURS));
        // 不配置或者超过最大值 24，取默认最大值 24
        rmIntervalHours = (rmIntervalHours == 0 || rmIntervalHours > DEFAULT_MAX_RM_INTERVAL_HOURS) ?
                DEFAULT_MAX_RM_INTERVAL_HOURS : rmIntervalHours;
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        records = null;
        try {
            int nread = channel.read(buffer);
            if (nread > 0) {
                buffer.flip();
                extractRecords();
            } else {
                logOffset();
                runtimeCheckOffset();
                checkFiles();
            }
            return records;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 从 buffer 中提取行封装成 Records
     */
    private void extractRecords() {
        int limit = buffer.limit();
        byte[] bytes = buffer.array();
        buffer.clear();

        int from, end = -1;
        for (int i = 0; i < limit; i++) {
            if (bytes[i] == '\n') {
                from = end + 1;
                end = i;
                String line = new String(bytes, from, end - from);
                position += (end - from) + 1;
                LOG.trace("Read a line: {} from {}", line, logFilename());

                int _r = line.lastIndexOf('\r');

                if (line.length() > 0 && _r == line.length() - 1) {
                    line = line.substring(0, _r);
                }

                if (line.length() > 0) {
                    if (records == null)
                        records = new ArrayList<>();
                    String value = scheme.deserialize(line);
                    if (StringUtils.isNotBlank(value)) {
                        records.add(new SourceRecord(offsetKey(), offsetValue(), topic, VALUE_SCHEMA, value));
                    }
                }
            }
        }
        buffer.put(bytes, end + 1, limit - (end + 1));
    }

    @Override
    public void stop() {
        LOG.trace("Stopping");
        synchronized (this) {
            try {
                if (channel != null) {
                    channel.close();
                    LOG.trace("Closed FileChannel of FileStreamSourceTask stream.");
                }
            } catch (IOException e) {
                LOG.error("Failed to close FileStreamSourceTask stream: ", e);
            }
            this.notify();
        }
    }

    private Map<String, String> offsetKey() {
        return Collections.singletonMap(partitionKey, partitionValue);
    }

    private Map<String, String> offsetValue() {
        //TODO - 存储 offset
        OffsetValue offsetValue = offset.get("0");
        offsetValue.setPosition(position);
        lastModifyTime = FileUtil.getLastModifyTime(path, filename);
        offsetValue.setLastModifyTime(DateUtils.getOffsetLastModifyTime(lastModifyTime));
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(CURRENT, JSON.toJSONString(offsetValue));

        filePositions.put(filename, position);
        offsetMap.put(FILES, JSON.toJSONString(filePositions));

        if (transferInodes) {
            offsetMap.put(INODES, JSON.toJSONString(fileKeys));
            transferInodes = false;
        }

        LOG.trace("send:" + offsetMap.toString());

        return offsetMap;
    }

    private void initOffset() {
        do {
            OffsetValue[] offsets = getOffsets(start);
            if (offsets == null || offsets.length == 0) {
                LOG.warn("Couldn't find file for FileStreamSourceTask, sleeping to wait (5s) for it to be created");
                try {
                    synchronized (this) {
                        this.wait(5000);
                    }
                } catch (InterruptedException e) {
                    System.exit(1);
                }
                continue;
            }
            for (int i = 0; i < offsets.length; i++) {
                if (offset == null) {
                    offset = new HashMap<>();
                }
                offset.put(i + "", offsets[i]);
                if (i == 0) {
                    transferInodes = true;
                }
            }

        } while (offset == null);
    }

    /**
     *
     */
    private void setupCheckOffset(){
        int size = offset.size();
        Map<String, OffsetValue> tempOffset = offset;
        offset = new HashMap<>();
        try {
            int j = 0;
            for (int i = 0; i < size; i++) {
                OffsetValue offsetValue = tempOffset.get(i + "");
                String filename = offsetValue.getFilename();
                if (Files.exists(Paths.get(path, filename))) {
                    BasicFileAttributes attr = Files.readAttributes(Paths.get(path, filename),
                            BasicFileAttributes.class);
                    long fileSize = attr.size();
                    long position = offsetValue.getPosition();
                    if (position <= fileSize) {
                        offset.put(j + "", offsetValue);
                    }
                }
            }

            if (offset.size() <= 0) {
                offset = null;
                initOffset();
            }
        } catch (IOException e) {
            LOG.error("Couldn't readAttributes from File:{} for FileStreamSourceTask!",
                    path + File.separator + filename);
            e.printStackTrace();
            System.exit(1);
        }
    }
    private void runtimeCheckOffset() {
        if (offset.size() > 2) {
            checkCounter = 0;
            // 关闭当前流, 删除 offset 中过期的 file， 开启下一个文件的采集流
            try {
                changeStreamTo(1);
                popOffset();
                if (!isRecursive)
                    LOG.info("current offset:{}", offset);
            } catch (IOException e) {
                //TODO 可能得尝试几次，或者等待几秒再尝试一次
                LOG.error("Couldn't open stream:{} for FileStreamSourceTask! 忽略这个文件",
                        path + File.separator + filename);
                e.printStackTrace();
                removeOffset(1);
                LOG.info("current offset:{}", offset);
            }
        } else if (offset.size() <= 2) {
            if (checkNextIsReady()) {
                checkCounter = 0;
                // 关闭当前流, 删除 offset 中过期的 file， 开启下一个文件的采集流
                try {
                    changeStreamTo(1);
                    popOffset();
                    if (!isRecursive)
                        LOG.info("current offset:{}", offset);
                } catch (IOException e) {
                    //TODO 可能得尝试几次，或者等待几秒再尝试一次
                    LOG.error("Couldn't open stream:{} for FileStreamSourceTask! 忽略这个文件",
                            path + File.separator + filename);
                    e.printStackTrace();
                    removeOffset(1);
                    LOG.info("current offset:{}", offset);
                }
            } else {
                OffsetValue before = offset.get("0");
                String filename = before.getFilename();
                String inode = before.getInode();
                long position = before.getPosition();

                checkAndAddNewFileToOffset();

                OffsetValue after = offset.get("0");
                if (!filename.equals(after.getFilename()) ||
                        !inode.equals(after.getInode()) ||
                        position != after.getPosition()){
                    try {
                        changeStreamTo(0);
                        LOG.info("checkAndAddNewFileToOffset 后，当前流的 channel 信息有变化！可能是重命名当前文件导致的：{}" +
                                "before check -> {filename:{}; inode:{}, position:{}};{}" +
                                "after check -> {filename:{}; inode:{}, position:{}}",
                                System.lineSeparator(), filename, inode, position,
                                System.lineSeparator(), after.getFilename(), after.getInode(), after.getPosition());
                    } catch (IOException e) {
                        LOG.error("切换当前文件流异常");
                        e.printStackTrace();
                    }
                }

                checkCounter ++;
                waitStrategy();
            }
        }
    }

    private void storeOffsets(Map<String, Object> storeOffset) {
        String current = (String) storeOffset.get(CURRENT);
        if (StringUtils.isNotBlank(current)) {
            if (offset == null) {
                offset = new HashMap<>();
            }
            OffsetValue currentOffsetValue = JSONObject.parseObject(current, OffsetValue.class);
            start = DateUtils.getOffsetLastModifyTime(currentOffsetValue);
            offset.put("0", currentOffsetValue);
        }

        String offsets = (String) storeOffset.get(FILES);
        if (StringUtils.isNotBlank(offsets)) {
            filePositions = JSONObject.parseObject(offsets);
        }

        String inodes = (String) storeOffset.get(INODES);
        if (StringUtils.isNotBlank(inodes)) {
            fileKeys = JSONObject.parseObject(inodes);
        }
    }

    private void popOffset() {
        removeOffset(0);
    }

    private void removeOffset(int key) {
        Map<String, OffsetValue> tempOffset = offset;
        offset = new HashMap<>();
        OffsetValue offsetValue;
        for (int i = 0; i < key; i++) {
            offsetValue = tempOffset.get(i + "");
            offset.put(i + "", offsetValue);
        }

        for (int i = key + 1; i < tempOffset.size(); i++) {
            offsetValue = tempOffset.get(i + "");
            offset.put(i - 1 + "", offsetValue);
        }
    }

    private boolean checkNextIsReady() {
        String next = 1 + "";
        if (offset.containsKey(next)) {
            OffsetValue offsetValue = offset.get(next);
            String nextFile = offsetValue.getFilename();
            long position = offsetValue.getPosition();

            if (Files.exists(Paths.get(path, nextFile))) {
                try {
                    BasicFileAttributes attr = Files.readAttributes(Paths.get(path, nextFile),
                            BasicFileAttributes.class);
                    long fileSize = attr.size();
                    return fileSize > position;
                } catch (IOException e) {
                    LOG.error("Couldn't readAttributes from File:{} for FileStreamSourceTask!",
                            path + File.separator + nextFile);
                    e.printStackTrace();
                    removeOffset(1);
                    return false;
                }

            } else {
                LOG.error("Couldn't find File:{} for FileStreamSourceTask!",
                        path + File.separator + nextFile);
                removeOffset(1);
                return false;
            }
        } else {
            return false;
        }
    }

    /**
     * 检查文件目录下新文件，并按照最后修改时间排序
     */
    private void checkAndAddNewFileToOffset() {
        OffsetValue currentOffsetValue = offset.get("0");
        lastModifyTime = DateUtils.getOffsetLastModifyTime(currentOffsetValue);

        OffsetValue[] offsets = getOffsets(lastModifyTime);
        if (offsets != null) {
            int size = offset.size();
            for (OffsetValue value : offsets) {
                String filename = value.getFilename();
                String inode = value.getInode();
                long position = value.getPosition();

                boolean isAdd = true;

                for (Map.Entry<String, OffsetValue> entry : offset.entrySet()) {
                    OffsetValue entryValue = entry.getValue();
                    if (inode.equals(entryValue.getInode()) && filename.equals(entryValue.getFilename())){
                        isAdd = false;
                        break;
                    }

                    if (inode.equals(entryValue.getInode()) && !filename.equals(entryValue.getFilename())) {
                        entryValue.setFilename(filename);
                        entryValue.setPosition(position);
                        isAdd = false;
                        if (!transferInodes)
                            transferInodes = true;
                        break;
                    }

                    if (!inode.equals(entryValue.getInode()) && filename.equals(entryValue.getFilename())) {
                        entryValue.setInode(inode);
                        entryValue.setPosition(position);
                        isAdd = false;
                        if (!transferInodes)
                            transferInodes = true;
                        break;
                    }
                }

                long fileSize= FileUtil.getFileSize(path, filename);
                if ((fileSize == 0L) ||
                        (filePositions.containsKey(filename) && fileSize == position)) {
                    isAdd = false;
                }

                if (isAdd) {
                    offset.put(size + "", value);
                    size++;

                    if (!transferInodes)
                        transferInodes = true;
                }
            }
        }
    }

    private OffsetValue[] getOffsets(LocalDateTime start) {

        ArrayList<OffsetValue> offsetList = null;
        try {
            Set<Path> paths = getFiles();
            for (Path file : paths) {
                String fileName = file.toString().substring(index);
                LocalDateTime lastModifyTime = FileUtil.getLastModifyTime(file);
                if (lastModifyTime == null)
                    continue;

                if (lastModifyTime.compareTo(start) >= 0) {
                    OffsetValue value = new OffsetValue(path, fileName, lastModifyTime);
                    value.setPosition(getPosition(value));
                    if (offsetList == null) {
                        offsetList = new ArrayList<>();
                    }
                    offsetList.add(value);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        OffsetValue[] offsets = null;
        if (offsetList != null) {
            offsets = new OffsetValue[offsetList.size()];
            offsetList.toArray(offsets);

            if (offsets.length > 1) {
                Arrays.sort(offsets, this::compareTo);
            }
        }

        return offsets;
    }

    private void changeStreamTo(int key) throws IOException {
        OffsetValue offsetValue = offset.get(key + "");
        String filename = offsetValue.getFilename();
        long position = offsetValue.getPosition();
        FileChannel fileChannel = FileChannel.open(Paths.get(path, filename), StandardOpenOption.READ);
        fileChannel.position(position);

        if (channel != null) {
            // 关闭当前流
            LOG.trace("Find offsets = {} for FileStreamSourceTask!", offset);
            LOG.trace("Close current FileChannel:{}", path + File.separator + this.filename);
            channel.close();
            LOG.trace("Open FileChannel:{} with position:{}", path + File.separator + filename, position);
        }

        this.channel = fileChannel;
        this.filename = filename;
        this.position = position;
    }

    private void removeOlderFromFiles() {
        if (filePositions == null || filePositions.size() <= 1) {
            return;
        }
        JSONObject tempFilePositions = filePositions;
        filePositions = new JSONObject();
        for (String filename : tempFilePositions.keySet()) {
            if (!filename.equals(this.filename)) {
                LocalDateTime fileLastModifyTime = FileUtil.getLastModifyTime(path, filename);
                if (fileLastModifyTime == null ||
                        LocalDateTime.now().minusHours(rmIntervalHours).compareTo(fileLastModifyTime) > 0) {
                    LOG.info("remove file: {fileName:{}, position:{}} from cache filePositions", filename, tempFilePositions.get(filename));
                    continue;
                }
            }
            filePositions.put(filename, tempFilePositions.get(filename));
        }

        JSONObject tempFileKeys = fileKeys;
        fileKeys = new JSONObject();
        for (String inode : tempFileKeys.keySet()) {
            String filename = tempFileKeys.getString(inode);
            if (!filename.equals(this.filename)) {
                LocalDateTime fileLastModifyTime = FileUtil.getLastModifyTime(path, filename);
                if (fileLastModifyTime == null ||
                        LocalDateTime.now().minusHours(rmIntervalHours).compareTo(fileLastModifyTime) > 0) {
                    LOG.info("remove file: {inode:{}, filename:{}} from cache fileKeys", inode, filename);
                    continue;
                }
            }
            fileKeys.put(inode, filename);
        }
        LOG.info("current fileKeys: {}", fileKeys);
        LOG.info("current filePositions: {}", filePositions);
    }


    private String logFilename() {
        return filename;
    }

    /**
     * 1. 判断 inode 是否存在
     *    + 存在：获取之前存储的 filename 比较 于现在的是否相同
     *       ++ 相同：获取 position 返回即可
     *       ++ 不相同： 说明文件被重命名了。获取之前的 filename->position，更新 filePositions, 返回 position
     *    + 不存在： 返货 filePositions 里存储 filename 的 position
     * @param offsetValue
     * @return
     */
    private long getPosition(OffsetValue offsetValue){
        String filename = offsetValue.getFilename();
        String inode = offsetValue.getInode();
        if (filePositions == null || filePositions.isEmpty()) {
            fileKeys.put(inode, filename);
            filePositions.put(filename, 0L);
            return 0L;
        }

        if (fileKeys.containsKey(inode)) {
            String file = fileKeys.getString(inode);
            if (file.equals(filename)) {
                return filePositions.getLongValue(file);
            } else {
                long position = filePositions.getLongValue(file);
                fileKeys.put(inode, filename);
                filePositions.put(filename, position);
                return position;
            }
        } else {
            fileKeys.put(inode, filename);

            // TODO 讨论
            if (filePositions.containsKey(filename)) {
                long position = filePositions.getLongValue(filename);
                long fileSize = FileUtil.getFileSize(path, filename);
                if (fileSize < position) {
                    filePositions.put(filename, 0L);
                }

//                filePositions.put(filename, 0L);
            }

            return filePositions.getLongValue(filename);
        }
    }

    /**
     * 输出当前 offset
     */
    private void logOffset() {
        if (System.currentTimeMillis() - logOffsetTime > 30 * 1000) {
            LOG.info("current offset: {}", offset);
            logOffsetTime = System.currentTimeMillis();
        }
    }

    /**
     * 定期检查文件修改时间并删除过期文件
     */
    private void checkFiles() {
        if (System.currentTimeMillis() - checkFilesTime > 60 * 60 * 1000) {
            removeOlderFromFiles();
            checkFilesTime = System.currentTimeMillis();
        }
    }

    private int compareTo(OffsetValue o1, OffsetValue o2) {
        return (o1.getLastModifyTime()).compareTo(o2.getLastModifyTime());
    }

    /**
     * 根据是否递归子目录及文件通配符过滤出文件列表
     * @return
     * @throws IOException
     */
    private Set<Path> getFiles() throws IOException {
        Set<Path> paths = new HashSet<>();
        if (isRecursive) {
            RecursivePath walk = new RecursivePath(filter);
            Files.walkFileTree(Paths.get(path), walk);
            paths.addAll(walk.paths);
            walk.clear();
        } else {
            DirectoryStream<Path> ds = Files.newDirectoryStream(Paths.get(path), filter);
            for (Path path : ds) {
                paths.add(path);
            }
            ds.close();
        }

        return paths;
    }

    /**
     * 采集到文件末尾的时的等待策略，根据重复检查新文件的次数设置不同的等待时间
     */
    private void waitStrategy(){
        if (checkCounter >= 10 && checkCounter < 50) {
            Thread.yield();
        }
        try {
            synchronized (this) {
                if (checkCounter >= 50 && checkCounter < 100) {
                    this.wait(100);
                }
                if (checkCounter >= 100 && checkCounter < 500) {
                    this.wait(500);
                }
                if (checkCounter >= 500 && checkCounter < 1000) {
                    this.wait(1000);
                }
                if (checkCounter >= 1000) {
                    this.wait(2000);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
