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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * FileStreamSourceTask reads from stdin or a file.
 */
public class FileStreamSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(org.apache.kafka.connect.file.FileStreamSourceTask.class);
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

    private String path;

//    private String currentTime;
    private String filePrefix;
    private String fileSuffix;
//    private String fileRoundUnit;
    private DateTimeFormatter format;
    private String filename;
    private String fileRegex;
//    private String nextfile;
    private InputStream stream;
    private BufferedReader reader = null;
    private char[] buffer = new char[1024];
    private int offset = 0;
    private String topic = null;

    private Long streamOffset;

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

        String fileDateFormat = props.get(FileStreamSource.FILE_DATE_CONFIG);
        format = DateTimeFormatter.ofPattern(fileDateFormat);

        filename = props.get(FileStreamSource.FILE_CONFIG);

        topic = props.get(FileStreamSource.TOPIC_CONFIG);
        if (topic == null)
            throw new ConnectException("FileStreamSourceTask config missing topic setting");

        partitionKey = "fileType: " + path + File.separator + fileRegex;
        partitionValue = "topic: " + topic;
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if (stream == null) {
            try {
                Map<String, Object> offset = context.offsetStorageReader().offset(offsetKey());
                stream = new FileInputStream(filename);

                if (offset != null) {
                    Object lastRecordedOffset = offset.get(filename);
                    if (lastRecordedOffset != null && !(lastRecordedOffset instanceof Long))
                        throw new ConnectException("Offset position is the incorrect type");
                    if (lastRecordedOffset != null) {
                        log.debug("Found previous offset, trying to skip to file offset {}", lastRecordedOffset);
                        long skipLeft = (Long) lastRecordedOffset;
                        while (skipLeft > 0) {
                            try {
                                long skipped = stream.skip(skipLeft);
                                skipLeft -= skipped;
                            } catch (IOException e) {
                                log.error("Error while trying to seek to previous offset in file: ", e);
                                throw new ConnectException(e);
                            }
                        }
                        log.debug("Skipped to offset {}", lastRecordedOffset);
                    }
                    streamOffset = (lastRecordedOffset != null) ? (Long) lastRecordedOffset : 0L;
                } else {
                    streamOffset = 0L;
                }
                reader = new BufferedReader(new InputStreamReader(stream));
                log.debug("Opened {} for reading", logFilename());
            } catch (FileNotFoundException e) {
                log.warn("Couldn't find file for FileStreamSourceTask, sleeping to wait for it to be created");
                synchronized (this) {
                    this.wait(1000);
                }
                return null;
            }
        }

        // Unfortunately we can't just use readLine() because it blocks in an uninterruptible way.
        // Instead we have to manage splitting lines ourselves, using simple backoff when no new data
        // is available.
        try {
            final BufferedReader readerCopy;
            synchronized (this) {
                readerCopy = reader;
            }
            if (readerCopy == null)
                return null;

            ArrayList<SourceRecord> records = null;

            int nread = 0;
            while (readerCopy.ready()) {
                nread = readerCopy.read(buffer, offset, buffer.length - offset);
                log.trace("Read {} bytes from {}", nread, logFilename());

                if (nread > 0) {
                    offset += nread;
                    if (offset == buffer.length) {
                        char[] newbuf = new char[buffer.length * 2];
                        System.arraycopy(buffer, 0, newbuf, 0, buffer.length);
                        buffer = newbuf;
                    }

                    String line;
                    do {
                        line = extractLine();
                        if (line != null) {
                            log.trace("Read a line from {}", logFilename());
                            if (records == null)
                                records = new ArrayList<>();
                            records.add(new SourceRecord(offsetKey(), offsetValue(), topic, VALUE_SCHEMA, line));
                        }
                        new ArrayList<SourceRecord>();
                    } while (line != null);
                }
            }

            if (nread <= 0)
                synchronized (this) {
                    this.wait(1000);
                }

            return records;
        } catch (IOException e) {
            // Underlying stream was killed, probably as a result of calling stop. Allow to return
            // null, and driving thread will handle any shutdown if necessary.
        }
        return null;
    }

    private String extractLine() {
        int until = -1, newStart = -1;
        for (int i = 0; i < offset; i++) {
            if (buffer[i] == '\n') {
                until = i;
                newStart = i + 1;
                break;
            } else if (buffer[i] == '\r') {
                // We need to check for \r\n, so we must skip this if we can't check the next char
                if (i + 1 >= offset)
                    return null;

                until = i;
                newStart = (buffer[i + 1] == '\n') ? i + 2 : i + 1;
                break;
            }
        }

        if (until != -1) {
            String result = new String(buffer, 0, until);
            System.arraycopy(buffer, newStart, buffer, 0, buffer.length - newStart);
            offset = offset - newStart;
            if (streamOffset != null)
                streamOffset += newStart;
            return result;
        } else {
            return null;
        }
    }

    @Override
    public void stop() {
        log.trace("Stopping");
        synchronized (this) {
            try {
                if (stream != null && stream != System.in) {
                    stream.close();
                    log.trace("Closed input stream");
                }
            } catch (IOException e) {
                log.error("Failed to close FileStreamSourceTask stream: ", e);
            }
            this.notify();
        }
    }

    private Map<String, String> offsetKey() {
        return Collections.singletonMap(partitionKey, partitionValue);
    }

    private Map<String, Long> offsetValue() {
        Map<String, Long> offsetMap = new HashMap<>();
        //TODO - 存储 offset
        return offsetMap;
    }

    private String logFilename() {
        return filename;
    }

//    private String getNextfile(){
//        return filePrefix + getNextTime() + fileSuffix;
//    }

//    private String getNextTime(){
//        switch (fileRoundUnit) {
//            case "minute" :
//                return LocalDateTime.parse(currentTime, format).plusMinutes(10).format(format);
//            case "hour" :
//                return LocalDateTime.parse(currentTime, format).plusHours(1).format(format);
//            case "day" :
//                return LocalDateTime.parse(currentTime, format).plusDays(1).format(format);
//            default :
//                return null;
//        }
//    }
}
