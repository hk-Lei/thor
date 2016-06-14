/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.emar.kafka.connect;

import com.emar.kafka.utils.DateUtils;
import io.netty.buffer.ByteBuf;
import org.apache.kafka.common.record.ByteBufferInputStream;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * FileStreamSourceTask reads from stdin or a file.
 */
public class FileStreamSourceTask extends SourceTask {
    private static final Logger LOG = LoggerFactory.getLogger(FileStreamSourceTask.class);
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

    private static final String FILE = "file";
    private static final String POSITION = "position";
    private static final String CREATION_TIME = "creation_time";
    private static final String LAST_MODIFY_TIME = "last_modify_time";

    private static final int _1M = 1024 * 1024;
    private static final int _5M = 5 * _1M;
    private static final int _10M = 10 * _1M;

    private String path;

    private String fileSuffix;
    private String filePrefix;
    private boolean ignoreOffset;
    private LocalDateTime start;
    private String fileRegex;
    private String filename;
    private long position;
    private long lineOffset;
    private LocalDateTime creationTime;
    private LocalDateTime lastModifyTime;
    private FileChannel channel;
    private InputStream stream;
    private BufferedReader reader = null;
    private ByteBuffer buffer = null;
    private ArrayList<SourceRecord> records;
    private String topic = null;

    private Map<String, Object> offset;

    private String partitionKey;
    private String partitionValue;

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
        String startTime = props.get(FileStreamSource.START_TIME);

        ignoreOffset = Boolean.parseBoolean(props.get(FileStreamSource.IGNORE_OFFSET));
        if (ignoreOffset) {
            LOG.warn("ignore.offset 为 true, 将忽略之前的 offset，以 start.time 为首要参考对象");
            start = getStart(startTime);
            initOffset();
        } else {
            LOG.warn("ignore.offset 为 false（默认为 false）, 将以之前的 offset 为首要参考对象");
            offset = context.offsetStorageReader().offset(offsetKey());
            if (offset == null) {
                LOG.warn("Local offset is null! 以 start.time 为参考对象");
                initOffset();
            } else {
                checkOffset();
            }
        }

        Map<String, Object> offsetValue = (Map<String, Object>) offset.get("0");
        filename = (String) offsetValue.get(FILE);
        position = (long) offsetValue.get(POSITION);

        try {
            channel = FileChannel.open(Paths.get(path, filename), StandardOpenOption.READ);
            channel.position(position);
        } catch (IOException e) {
            e.printStackTrace();
        }

        topic = props.get(FileStreamSource.TOPIC_CONFIG);
        if (topic == null)
            throw new ConnectException("FileStreamSourceTask config missing topic setting");

        partitionKey = "fileType: " + this.path + File.separator + fileRegex;
        partitionValue = "topic: " + topic;
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        records = null;
        ByteBuffer buffer = ByteBuffer.allocate(_5M);

        try {
            int nread = channel.read(buffer);
            LOG.trace("Read {} bytes from {}", nread, logFilename());
            if (nread > 0) {
                buffer.flip();
                records = extractRecords();
                lineOffset += nread;
//                String line;
//                do {
//                    line = extractLine();
//                    if (line != null) {
//                        LOG.trace("Read a line from {}", logFilename());
//                        if (records == null)
//                            records = new ArrayList<>();
//                        records.add(new SourceRecord(offsetKey(), offsetValue(), topic, VALUE_SCHEMA, line));
//                    }
//                    new ArrayList<SourceRecord>();
//                } while (line != null);
            }
            return records;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    private ArrayList<SourceRecord> extractRecords() {
        int limit = buffer.limit();
        byte[] bytes = buffer.array();
        int from, end = -1;
        for (int i = 0; i < limit; i++) {
            if (bytes[i] == '\n') {
                from = end + 1;
                end = i;
                String line = new String(bytes, from, end - from);
                LOG.trace("Read a line: {} from {}", line, logFilename());
                if (records == null)
                    records = new ArrayList<>();
                records.add(new SourceRecord(offsetKey(), offsetValue(), topic, VALUE_SCHEMA, line));
            }
        }
        buffer.compact();

//
//        if (until != -1) {
//            String result = new String(buffer, 0, until);
//            System.arraycopy(buffer, newStart, buffer, 0, buffer.length - newStart);
//            lineOffset = lineOffset - newStart;
//
//            position += newStart;
//            return result;
//        } else {
//            return null;
//        }
        return null;
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

    private Map<String, Object> offsetValue() {
        //TODO - 存储 offset
        return offset;
    }

    private LocalDateTime getStart(String startTime){
        DateTimeFormatter format = DateUtils.getFormatter(startTime);
        if (startTime == null || startTime.isEmpty() || format == null){
            LOG.warn("start.time 为空或者非法（格式必须为 yyyyMMddHHmm 或 yyyyMMddHH 或者 yyyyMMdd），将 start.time 设置为当前时间精确到分钟");
            // 开始时间精确到小时
            return LocalDateTime.now().withSecond(0);
        } else
            return LocalDateTime.parse(startTime, format);
    }

    /**
     * TODO 需要对 offset 按 creationTime 排序，时间最小的 key 为 0
     */
    private void initOffset() {
        do {
            int i = 0;
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(Paths.get(path), fileRegex)) {
                for (Path file : ds) {
                    BasicFileAttributes attr = Files.readAttributes(file, BasicFileAttributes.class);
                    LocalDateTime creationTime = getLocalDateTime(attr.creationTime());
                    LocalDateTime lastModifyTime = getLocalDateTime(attr.lastModifiedTime());
                    if (creationTime.compareTo(start) >= 0) {
                        if (i == 0) {
                            offset = new HashMap<>();
                        }
                        Map<String, Object> valueMap = new HashMap<>();
                        valueMap.put(FILE, file.getFileName().toString());
                        valueMap.put(POSITION, 0L);
                        valueMap.put(CREATION_TIME, creationTime);
                        valueMap.put(LAST_MODIFY_TIME, lastModifyTime);
                        offset.put(i + "", valueMap);
                        i++;
                    }
                }
                ds.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (offset == null) {
                LOG.warn("Couldn't find file for FileStreamSourceTask, sleeping to wait (2s) for it to be created");
                try {
                    synchronized (this) {
                        this.wait(2000);
                    }
                } catch (InterruptedException e) {
                    System.exit(1);
                }
            }
        } while (offset != null);
    }

    //TODO 检查 offset 对象的 file 是否存在，删除已经不存在的 file 或者过期的 file
    private void checkOffset(){

    }

    private LocalDateTime getLocalDateTime(FileTime time) {
        return LocalDateTime.ofInstant(time.toInstant(), ZoneId.of("Asia/Shanghai"));
    }

    private String logFilename() {
        return filename;
    }
}
