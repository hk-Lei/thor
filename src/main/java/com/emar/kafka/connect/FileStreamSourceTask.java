package com.emar.kafka.connect;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.emar.kafka.interceptor.Scheme;
import com.emar.kafka.offset.Offset;
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
    private String fileRegex;
    private int index;
    private int checkCounter;
    private DirectoryStream.Filter<Path> filter;
    private boolean isRecursive;
    private int rmIntervalHours;
    private LocalDateTime start;
    private String filename;
    private long position;
    private FileChannel channel;
    private ArrayList<SourceRecord> records;
    private String topic = null;

    private Scheme scheme = null;
    private LinkedList<Offset> queue;
    private JSONObject filePositions;
    private JSONObject fileKeys;

    private String partitionKey;
    private String partitionValue;
    private boolean transferInodes;

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
        fileRegex = filePrefix + "*" + fileSuffix;
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
            LOG.info("采集的日志：path={}; fileType={}; topic={}; start.time={}", path, fileRegex, topic, start);
            initOffset();
        } else {
            LOG.warn("ignore.offset 为 false（默认为 false）, 将以之前的 offset 为首要参考对象");
            Map<String, Object> storeOffset = context.offsetStorageReader().offset(offsetKey());
            if (storeOffset == null) {
                LOG.warn("Local stored offset is null! 以 start.time 为参考对象");
                start = DateUtils.getStart(startTime);
                initOffset();
                LOG.info("采集的日志：path={}; fileType={}; topic={}; start.time={}", path, fileRegex, topic, start);
            } else {
                getStoreOffset(storeOffset);
            }
        }
        LOG.info("current offsets:{}", queue);
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
        removeOlderFromFiles();
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
                LOG.trace("Read a line: {} from {}", line, filename);

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

    public Map<String, String> offsetKey() {
        return Collections.singletonMap(partitionKey, partitionValue);
    }

    /**
     * 存储的数据：
     *      current:
     *          {filename:$filename,inode:$inode,position:$position,lastModifyTime:$lastModifyTime}
     *      file->position:
     *          {$filename:$position, ...}
     *      inode->file:
     *          {$inode:$filename, ...}
     * @return
     */
    public Map<String, String> offsetValue() {
        //TODO - 存储 offset
        Offset offsetValue = queue.getFirst();
        offsetValue.setPosition(position);
        LocalDateTime lastModifyTime = FileUtil.getLastModifyTime(path, filename);
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

    /**
     * 获取 最后修改时间 >= start 的
     */
    private void initOffset() {
        do {
            Offset[] offsets = getOffsets(start);
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
                if (queue == null) {
                    queue = new LinkedList<>();
                }
                queue.add(offsets[i]);
                if (i == 0) {
                    transferInodes = true;
                }
            }

        } while (queue == null);
    }

    private void runtimeCheckOffset() {
        if (queue.size() > 2) {
            checkCounter = 0;
            // 关闭当前流, 删除 offset 中过期的 file， 开启下一个文件的采集流
            try {
                changeStreamTo(1);
                queue.removeFirst();
                if (!isRecursive)
                    LOG.info("RuntimeCheckOffsets: {}", queue);
            } catch (IOException e) {
                //TODO 可能得尝试几次，或者等待几秒再尝试一次
                LOG.error("Couldn't open stream:{} for FileStreamSourceTask! 忽略这个文件",
                        path + File.separator + filename);
                e.printStackTrace();
                queue.remove(1);
                LOG.info("RuntimeCheckedOffsets: {}", queue);
            }
        } else if (queue.size() <= 2) {
            if (checkNextIsReady()) {
                checkCounter = 0;
                // 关闭当前流, 删除 offset 中过期的 file， 开启下一个文件的采集流
                try {
                    changeStreamTo(1);
                    queue.removeFirst();
                    if (!isRecursive)
                        LOG.info("RuntimeCheckOffsets: {}", queue);
                } catch (IOException e) {
                    //TODO 可能得尝试几次，或者等待几秒再尝试一次
                    LOG.error("Couldn't open stream:{} for FileStreamSourceTask! 忽略这个文件",
                            path + File.separator + filename);
                    e.printStackTrace();
                    queue.remove(1);
                    LOG.info("RuntimeCheckOffsets: {}", queue);
                }
            } else {
                Offset before = queue.getFirst();
                String filename = before.getFilename();
                String inode = before.getInode();
                long position = before.getPosition();
                LocalDateTime lastModifiedTime = DateUtils.getOffsetLastModifyTime(before);

                checkAndAddNewFileToOffset(lastModifiedTime);

                Offset after = queue.getFirst();
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
                        LOG.info("current offsets: {}", queue);
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

    /**
     * 获取存储的 offset
     * @param storeOffset
     */
    private void getStoreOffset(Map<String, Object> storeOffset) {
        String current = (String) storeOffset.get(CURRENT);

        Offset currentOffset = JSONObject.parseObject(current, Offset.class);
        start = DateUtils.getOffsetLastModifyTime(currentOffset);
        LOG.info("采集的日志：path={}; fileType={}; topic={}; start.time={}", path, fileRegex, topic, start);

        String offsets = (String) storeOffset.get(FILES);
        filePositions = JSONObject.parseObject(offsets);

        String inodes = (String) storeOffset.get(INODES);
        fileKeys = JSONObject.parseObject(inodes);

        checkStoreOffset(currentOffset);
    }

    /**
     * 检查启动时的 offset
     *  + 如果存储的 offset 的文件不存在 删除
     *  + 如果存储的 offset 的 position > 文件的大小 暂且假设落日志的主文件名不变
     */
    private void checkStoreOffset(Offset offset){
        LOG.info("Got store offset: {}", offset);
        String path = offset.getPath();
        String filename = offset.getFilename();
        Path file = Paths.get(path, filename);
        String inode = offset.getInode();
        long position = offset.getPosition();

        if (queue == null) {
            queue = new LinkedList<>();
        }

        if (Files.exists(file)) {
            if (inode.equals(FileUtil.getFileInode(file))) {
                if (position < FileUtil.getFileSize(file))
                    queue.add(offset);
                else {
                    initOffset();
                }
            } else {
                if (fileKeys.containsKey(inode)) {
                    offset.setFilename(fileKeys.getString(inode));
                    queue.add(offset);
                } else {
                    initOffset();
                }
            }
        } else {
            initOffset();
        }
    }

    /**
     * 检查下一个日志文件是否准备就绪
     * @return
     */
    private boolean checkNextIsReady() {
        if (queue.size() > 1) {
            Offset offset = queue.get(1);
            String nextFile = offset.getFilename();
            long position = offset.getPosition();

            if (Files.exists(Paths.get(path, nextFile))) {
                long fileSize = FileUtil.getFileSize(Paths.get(path, nextFile));
                return fileSize > position;
            } else {
                LOG.error("Couldn't find File:{} for FileStreamSourceTask!",
                        path + File.separator + nextFile);
                queue.remove(1);
                return false;
            }
        } else
            return false;
    }

    /**
     * 检查文件目录下新文件，并按照最后修改时间排序
     * @param start
     */
    private void checkAndAddNewFileToOffset(LocalDateTime start) {
        Offset[] offsets = getOffsets(start);
        if (offsets != null) {
            for (Offset value : offsets) {
                String filename = value.getFilename();
                String inode = value.getInode();
                long position = value.getPosition();

                boolean isAdd = true;

                for (Offset offset : queue) {
                    if (inode.equals(offset.getInode()) && filename.equals(offset.getFilename())){
                        isAdd = false;
                        break;
                    }

                    if (inode.equals(offset.getInode()) && !filename.equals(offset.getFilename())) {
                        offset.setFilename(filename);
                        offset.setPosition(position);
                        isAdd = false;
                        if (!transferInodes)
                            transferInodes = true;
                        break;
                    }

                    if (!inode.equals(offset.getInode()) && filename.equals(offset.getFilename())) {
                        offset.setInode(inode);
                        offset.setPosition(position);
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
                    queue.add(value);
                    if (!transferInodes)
                        transferInodes = true;
                }
            }
        }
    }

    private Offset[] getOffsets(LocalDateTime start) {
        ArrayList<Offset> offsetList = null;
        try {
            Set<Path> paths = getFiles();
            for (Path file : paths) {
                String fileName = file.toString().substring(index);
                LocalDateTime lastModifyTime = FileUtil.getLastModifyTime(file);
                if (lastModifyTime == null)
                    continue;

                if (lastModifyTime.compareTo(start) >= 0) {
                    Offset value = new Offset(path, fileName, lastModifyTime);
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
        Offset[] offsets = null;
        if (offsetList != null) {
            offsets = new Offset[offsetList.size()];
            offsetList.toArray(offsets);

            if (offsets.length > 1) {
                Arrays.sort(offsets, this::compareTo);
            }
        }

        return offsets;
    }

    private void changeStreamTo(int index) throws IOException {
        Offset offset = queue.get(index);
        String filename = offset.getFilename();
        long position = offset.getPosition();
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

    /**
     * 删除过期文件，防止 filePositions 过大！
     */
    private void removeOlderFromFiles() {
        JSONObject tempFilePositions = filePositions;
        filePositions = new JSONObject();
        for (String filename : tempFilePositions.keySet()) {
            if (!filename.equals(this.filename)) {
                LocalDateTime fileLastModifyTime = FileUtil.getLastModifyTime(path, filename);
                LOG.info("rmIntervalHours: {}", rmIntervalHours);
                if (fileLastModifyTime == null ||
                        LocalDateTime.now().minusHours(rmIntervalHours).compareTo(fileLastModifyTime) > 0) {
                    LOG.info("remove file: {fileName:{}, position:{}} from cache filePositions", filename, tempFilePositions.get(filename));
                } else
                    filePositions.put(filename, tempFilePositions.get(filename));
            } else
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
                } else
                    fileKeys.put(inode, filename);
            } else
                fileKeys.put(inode, filename);
        }
        LOG.info("current fileKeys: {}", fileKeys);
        LOG.info("current filePositions: {}", filePositions);
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
    private long getPosition(Offset offsetValue){
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
            // TODO 讨论
            if (filePositions.containsKey(filename)) {
                long position = filePositions.getLongValue(filename);
                long fileSize = FileUtil.getFileSize(path, filename);
                if (fileSize < position) {
                    filePositions.put(filename, 0L);
                } else {
                    if (fileKeys.size() > 0) {
                        Collection<Object> values = fileKeys.values();
                        Set<String> inodes = new HashSet<>();
                        if (values.contains(filename)) {
                            fileKeys.keySet().forEach(i -> {
                                if (fileKeys.get(i).equals(filename)) {
                                    inodes.add(i);
                                }
                            });
                            inodes.forEach(i -> fileKeys.remove(i));
                        }
                    }
                }
            }
            fileKeys.put(inode, filename);
            return filePositions.getLongValue(filename);
        }
    }

    /**
     * 输出当前 offset
     */
    private void logOffset() {
        if (System.currentTimeMillis() - logOffsetTime > 30 * 1000) {
            LOG.info("current offsets: {}", queue);
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

    private int compareTo(Offset o1, Offset o2) {
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
