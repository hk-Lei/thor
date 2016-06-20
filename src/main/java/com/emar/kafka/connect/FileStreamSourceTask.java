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

import com.alibaba.fastjson.JSONObject;
import com.emar.kafka.interceptor.Scheme;
import com.emar.kafka.offset.OffsetValue;
import com.emar.kafka.utils.ConfigUtil;
import com.emar.kafka.utils.DateUtils;
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

    private static final int _1M = 1024 * 1024;
    private static final int _5M = 5 * _1M;
    private static final int _10M = 10 * _1M;
    private static ByteBuffer buffer = null;

    private String path;
    private String fileSuffix;
    private String filePrefix;
    private boolean ignoreOffset;
    private LocalDateTime start;
    private LocalDateTime lastModifyTime;
    private String fileRegex;
    private String filename;
    private long position;
    private FileChannel channel;
    private ArrayList<SourceRecord> records;
    private String topic = null;
    private Scheme scheme = null;

    private Map<String, OffsetValue> offset;

    private String partitionKey;
    private String partitionValue;

    private long logOffsets = System.currentTimeMillis() / 1000;

    @Override
    public String version() {
        return new FileStreamSource().version();
    }

    @Override
    public void start(Map<String, String> props) {
        path = props.get(FileStreamSource.PATH_CONFIG);
        filePrefix = props.get(FileStreamSource.FILE_PREFIX_CONFIG);
        fileSuffix = props.get(FileStreamSource.FILE_SUFFIX_CONFIG);
        fileRegex = filePrefix + "*" + fileSuffix;

        topic = props.get(FileStreamSource.TOPIC_CONFIG);
        if (topic == null) {
            throw new ConnectException("FileStreamSourceTask config missing topic setting");
        }
        partitionKey = "fileType: " + this.path + File.separator + fileRegex;
        partitionValue = "topic: " + topic;

        String startTime = props.get(FileStreamSource.START_TIME);
        start = DateUtils.getStart(startTime);
        String schemeClass = props.get(FileStreamSource.INTERCEPTOR_SCHEME);
        scheme = ConfigUtil.getInterceptorClass(schemeClass);

        ignoreOffset = Boolean.parseBoolean(props.get(FileStreamSource.IGNORE_OFFSET));
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
                storeOffsets2Offsets(storeOffset);
                checkOffset(true);
            }
        }

        try {
            changeStreamTo(0);
        } catch (IOException e) {
            LOG.error("Couldn't open stream:{} for FileStreamSourceTask!",
                    path + File.separator + filename);
            System.exit(1);
        }

        buffer = ByteBuffer.allocate(_5M);
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
                checkOffset(false);
                lastModifyTime = DateUtils.getFileLastModifyTime(path, filename);
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

                if (_r == line.length() - 1) {
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
        return Collections.singletonMap("0", offsetValue.toString());
    }

    private void initOffset() {
        do {
            OffsetValue[] offsets = getOffsets(start);
            if (offsets == null || offsets.length == 0) {
                LOG.warn("Couldn't find file for FileStreamSourceTask, sleeping to wait (2s) for it to be created");
                try {
                    synchronized (this) {
                        this.wait(2000);
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

    private void checkOffset(boolean setup) {
        if (setup) {
            int size = offset.size();
            Map<String, OffsetValue> tempOffset = offset;
            offset = new HashMap<>();
            try {
                int j = 0;
                for (int i = 0; i < size; i++) {
                    OffsetValue offsetValue = tempOffset.get(i + "");
                    String filename = offsetValue.getFile();
                    if (Files.exists(Paths.get(path, filename))) {
                        BasicFileAttributes attr = Files.readAttributes(Paths.get(path, filename), BasicFileAttributes.class);
                        long fileSize = attr.size();
                        long position = offsetValue.getPosition();
                        if (position <= fileSize){
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
        } else {
            if (offset.size() > 2) {
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
                }
            }

            if (System.currentTimeMillis() / 1000 - logOffsets >= 30) {
                LOG.info("current offset: {}", offset);
                logOffsets = System.currentTimeMillis();
            }
        }
    }

    private void storeOffsets2Offsets(Map<String, Object> storeOffsets){
        for (String key : storeOffsets.keySet()) {
            if (offset == null)
                offset = new HashMap<>();
            String offsetValue = (String) storeOffsets.get(key);
            offset.put(key, JSONObject.parseObject(offsetValue, OffsetValue.class));
        }
    }

    private void popOffset(){
        Map<String, OffsetValue> tempOffset = offset;
        offset = new HashMap<>();
        OffsetValue offsetValue;
        for (int i = 1; i < tempOffset.size(); i++) {
            offsetValue = tempOffset.get(i + "");
            offset.put(i - 1 + "", offsetValue);
        }
    }

    private void removeOffset(int key){
        Map<String, OffsetValue> tempOffset = offset;
        offset = new HashMap<>();
        OffsetValue offsetValue;
        for (int i = 0; i < key; i++) {
            offsetValue =  tempOffset.get(i + "");
            offset.put(i + "", offsetValue);
        }

        for (int i = key + 1; i < tempOffset.size(); i++) {
            offsetValue = tempOffset.get(i + "");
            offset.put(i - 1 + "", offsetValue);
        }
    }

    private boolean checkNextIsReady(){
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

    private void checkAndAddNewFileToOffset(){
        OffsetValue currentOffsetValue = offset.get("0");
        OffsetValue[] offsets = getOffsets(DateUtils.getOffsetLastModifyTime(currentOffsetValue));
        if (offsets != null) {
            for (int i = 1; i < offsets.length; i++) {
                offset.put(i + "", offsets[i]);
            }
        }
    }

    private OffsetValue[] getOffsets(LocalDateTime start){
        ArrayList<OffsetValue> offsetList = null;
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(Paths.get(path), fileRegex)) {
            for (Path file : ds) {
                LocalDateTime lastModifyTime = DateUtils.getFileLastModifyTime(file);
                if (lastModifyTime == null)
                    continue;

                if (lastModifyTime.compareTo(start) >= 0) {
                    OffsetValue value = new OffsetValue(file.getFileName().toString(), 0L, lastModifyTime);
                    if (offsetList == null){
                        offsetList = new ArrayList<>();
                    }
                    offsetList.add(value);
                }
            }
            ds.close();
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
        LOG.info("Open FileChannel:{} with position:{}", path + File.separator + file, position);
        FileChannel fileChannel = FileChannel.open(Paths.get(path, file), StandardOpenOption.READ);
        fileChannel.position(position);

        if (channel != null) {
            // 关闭当前流
            LOG.info("Find offsets = {} for FileStreamSourceTask!", offset);
            LOG.info("Close current FileChannel:{}", path + File.separator + filename);
            channel.close();
        }
        channel = fileChannel;
        filename = file;
        this.position = position;
    }

    private String logFilename() {
        return filename;
    }

    private int compareTo(OffsetValue o1, OffsetValue o2) {
        return ( o1.getLastModifyTime()).compareTo( o2.getLastModifyTime());
    }
}
