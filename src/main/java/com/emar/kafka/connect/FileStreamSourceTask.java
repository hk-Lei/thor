/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.emar.kafka.connect;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.emar.kafka.interceptor.Scheme;
import com.emar.kafka.offset.OffsetValue;
import com.emar.kafka.utils.ConfigUtil;
import com.emar.kafka.utils.DateUtils;
import com.emar.kafka.utils.RecursivePath;
import com.emar.kafka.utils.StringUtils;
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
    private static final String FILES = "files";

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
    private LocalDateTime start;
    private LocalDateTime lastModifyTime;
    private String filename;
    private long position;
    private FileChannel channel;
    private ArrayList<SourceRecord> records;
    private String topic = null;
    private Scheme scheme = null;

    private Map<String, OffsetValue> offset;
    private JSONObject files;

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
        filter = entry -> matcher.matches(entry.getFileName());

        topic = props.get(FileStreamSource.TOPIC_CONFIG);
        if (topic == null) {
            throw new ConnectException("FileStreamSourceTask config missing topic setting");
        }
        partitionKey = "fileType: " + this.path + File.separator + fileRegex;
        partitionValue = "topic: " + topic;
        String startTime = props.get(FileStreamSource.START_TIME);
        start = DateUtils.getStart(startTime);

        LOG.info("采集的日志：path={}; fileType={}; topic={}; start.time={}", path, fileRegex, topic, start);

        String schemeClass = props.get(FileStreamSource.INTERCEPTOR_SCHEME);
        scheme = ConfigUtil.getInterceptorClass(schemeClass);

        isRecursive = Boolean.parseBoolean(props.get(FileStreamSource.IS_RECURSIVE_DIR));

        boolean ignoreOffset = Boolean.parseBoolean(props.get(FileStreamSource.IGNORE_OFFSET));
        if (ignoreOffset) {
            LOG.warn("ignore.offset 为 true, 将忽略之前的 offset，以 start.time 为首要参考对象");
            initOffset();
        } else {
            LOG.warn("ignore.offset 为 false（默认为 false）, 将以之前的 offset 为首要参考对象");
            Map<String, Object> storeOffset = context.offsetStorageReader().offset(offsetKey());
            if (storeOffset == null) {
                LOG.warn("Local stored offset is null! 以 start.time 为参考对象");
                initOffset();
            } else {
                storeOffsets(storeOffset);
                setupCheckOffset();
            }
        }

        try {
            changeStreamTo(0);
        } catch (IOException e) {
            LOG.error("Couldn't open stream:{} for FileStreamSourceTask!",
                    path + File.separator + filename);
            System.exit(1);
        }
        buffer = ByteBuffer.allocate(_1M);
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
                runtimeCheckOffset();
                logOffset();
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
                    records.add(new SourceRecord(offsetKey(), offsetValue(), topic, VALUE_SCHEMA, scheme.deserialize(line)));
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
        offsetValue.setFile(filename);
        offsetValue.setPosition(position);
        offsetValue.setLastModifyTime(DateUtils.getOffsetLastModifyTime(lastModifyTime));
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(CURRENT, JSON.toJSONString(offsetValue));
        if (files == null)
            files = new JSONObject();
        files.put(filename, position);
        offsetMap.put(FILES, JSON.toJSONString(files));
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
            }
            LOG.info("current offset:{}", offset);
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
                String filename = offsetValue.getFile();
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
            } catch (IOException e) {
                //TODO 可能得尝试几次，或者等待几秒再尝试一次
                LOG.error("Couldn't open stream:{} for FileStreamSourceTask! 忽略这个文件",
                        path + File.separator + filename);
                e.printStackTrace();
                removeOffset(1);
            }
        } else if (offset.size() <= 2) {
            if (checkNextIsReady()) {
                checkCounter = 0;
                // 关闭当前流, 删除 offset 中过期的 file， 开启下一个文件的采集流
                try {
                    changeStreamTo(1);
                    popOffset();
                } catch (IOException e) {
                    //TODO 可能得尝试几次，或者等待几秒再尝试一次
                    LOG.error("Couldn't open stream:{} for FileStreamSourceTask! 忽略这个文件",
                            path + File.separator + filename);
                    e.printStackTrace();
                    removeOffset(1);
                }
            } else {
                checkAndAddNewFileToOffset();
                checkCounter ++;
                waitStrategy();
            }
        }
        lastModifyTime = DateUtils.getFileLastModifyTime(path, filename);
    }

    private void storeOffsets(Map<String, Object> storeOffset) {
        String current = (String) storeOffset.get(CURRENT);
        if (StringUtils.isNotBlank(current)) {
            if (offset == null) {
                offset = new HashMap<>();
            }
            offset.put("0", JSONObject.parseObject(current, OffsetValue.class));
        }

        String offsets = (String) storeOffset.get(FILES);
        if (StringUtils.isNotBlank(current)) {
            files = JSONObject.parseObject(offsets);
        }
    }

    private void popOffset() {
        Map<String, OffsetValue> tempOffset = offset;
        offset = new HashMap<>();
        OffsetValue offsetValue;
        for (int i = 1; i < tempOffset.size(); i++) {
            offsetValue = tempOffset.get(i + "");
            offset.put(i - 1 + "", offsetValue);
        }
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
            String nextFile = offsetValue.getFile();

            if (Files.exists(Paths.get(path, nextFile))) {
                try {
                    BasicFileAttributes attr = Files.readAttributes(Paths.get(path, nextFile),
                            BasicFileAttributes.class);
                    long fileSize = attr.size();
                    return fileSize > 0;
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

    private void checkAndAddNewFileToOffset() {
        lastModifyTime = DateUtils.getFileLastModifyTime(path, filename);
        OffsetValue[] offsets = getOffsets(lastModifyTime);
        if (offsets != null) {
            int size = offset.size();
            for (OffsetValue value : offsets) {
                boolean isAdd = true;
                for (Map.Entry<String, OffsetValue> entry : offset.entrySet()) {
                    OffsetValue entryValue = entry.getValue();
                    if (entryValue.getFile().equals(value.getFile())) {
                        isAdd = false;
                        break;
                    }
                }
                long fileSize= 0L;
                try {
                    BasicFileAttributes attr = Files.readAttributes(Paths.get(path, value.getFile()),
                            BasicFileAttributes.class);
                    fileSize = attr.size();
                } catch (IOException e) {
                    LOG.error("Couldn't readAttributes from File:{} for FileStreamSourceTask!",
                            path + File.separator + value.getFile());
                    e.printStackTrace();
                }
                if (fileSize <= getPosition(value.getFile()))
                    isAdd = false;

                if (isAdd) {
                    offset.put(size + "", value);
                    size++;
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
                LocalDateTime lastModifyTime = DateUtils.getFileLastModifyTime(file);
                if (lastModifyTime == null)
                    continue;

                if (lastModifyTime.compareTo(start) >= 0) {
                    OffsetValue value = new OffsetValue(fileName, getPosition(fileName), lastModifyTime);
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
        String file = offsetValue.getFile();
        long position = offsetValue.getPosition();
        FileChannel fileChannel = FileChannel.open(Paths.get(path, file), StandardOpenOption.READ);
        fileChannel.position(position);

        if (channel != null) {
            // 关闭当前流
            LOG.trace("Find offsets = {} for FileStreamSourceTask!", offset);
            LOG.trace("Close current FileChannel:{}", path + File.separator + filename);
            channel.close();
            LOG.trace("Open FileChannel:{} with position:{}", path + File.separator + file, position);
        }
        channel = fileChannel;
        filename = file;
        lastModifyTime = DateUtils.getFileLastModifyTime(path, filename);
        this.position = position;
    }

    private void removeOlderFromFiles() {
        if (files == null || files.size() <= 1) {
            return;
        }

        JSONObject temp = files;
        files = new JSONObject();
        for (String file : temp.keySet()) {
            if (!file.equals(filename)) {
                LocalDateTime fileLastModifyTime = DateUtils.getFileLastModifyTime(path, file);
                if (fileLastModifyTime == null ||
                        LocalDateTime.now().minusHours(24).compareTo(fileLastModifyTime) > 0) {
                    LOG.info("remove file: {fileName:{}, position:{}}", file, temp.get(file));
                    continue;
                }
            }
            files.put(file, temp.get(file));
        }
        LOG.info("current files: {}", files);
    }


    private String logFilename() {
        return filename;
    }

    private long getPosition(String file){
        if (files == null) {
            return 0L;
        } else {
            return files.getLongValue(file);
        }
    }

    /**
     * 输出当前 offset
     */
    private void logOffset() {
        if (System.currentTimeMillis() - logOffsetTime > 60 * 1000) {
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
        Set<Path> paths = null;
        if (isRecursive) {
            RecursivePath walk = new RecursivePath(filter);
            Files.walkFileTree(Paths.get(path), walk);
            for (Path path : walk.paths) {
                if (paths == null)
                    paths = new HashSet<>();
                paths.add(path);
            }
            walk.clear();
        } else {
            DirectoryStream<Path> ds = Files.newDirectoryStream(Paths.get(path), filter);
            for (Path path : ds) {
                if (paths == null)
                    paths = new HashSet<>();
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
        if (checkCounter >= 50 && checkCounter < 100) {
            Thread.yield();
        }
        try {
            synchronized (this) {
                if (checkCounter >= 100 && checkCounter < 500) {
                    this.wait(100);
                }
                if (checkCounter >= 500 && checkCounter < 1000) {
                    this.wait(500);
                }
                if (checkCounter >= 1000 && checkCounter < 5000) {
                    this.wait(1000);
                }
                if (checkCounter >= 5000) {
                    this.wait(5000);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
