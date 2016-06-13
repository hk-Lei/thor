package com.emar.kafka.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author moxingxing
 * @Date 2016/6/2
 */
public class FileStreamSource extends SourceConnector {

    public static final String TOPIC_CONFIG = "topic";
    public static final String PATH_CONFIG = "path";
    public static final String FILE_PREFIX_CONFIG = "file.prefix";
    public static final String FILE_SUFFIX_CONFIG = "file.suffix";
//    public static final String FILE_DATE_CONFIG = "file.date";
    public static final String FILE_CONFIG = "file";
    //    public static final String FILE_ROUNDUNIT = "file.roundUnit";
    public static final String START_TIME = "start.time";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(PATH_CONFIG, Type.STRING, Importance.HIGH, "Source path.")
            .define(START_TIME, Type.STRING, Importance.LOW, "Source start time.")
//            .define(FILE_ROUNDUNIT, Type.STRING, Importance.MEDIUM, "Source file round unit.")
//            .define(FILE_DATE_CONFIG, Type.STRING, Importance.MEDIUM, "Source filename date formater.")
            .define(FILE_PREFIX_CONFIG, Type.STRING, Importance.MEDIUM, "Source filename prefix.")
            .define(FILE_SUFFIX_CONFIG, Type.STRING, Importance.LOW, "Source filename suffix.")
            .define(FILE_CONFIG, Type.STRING, Importance.HIGH, "Source filename.")
            .define(TOPIC_CONFIG, Type.STRING, Importance.HIGH, "The topic to publish data to");

    private String path;
    private String startTime;
    private String filename;
    private String filePrefix;
    private String fileSuffix;
//    private String fileDateFormatter;
//    private String fileRoundUnit;
    private String topic;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        path = props.get(PATH_CONFIG);
        startTime = props.get(START_TIME);

//        fileDateFormatter = props.get(FILE_DATE_CONFIG);
        DateTimeFormatter format = DateTimeFormatter.ofPattern(fileDateFormatter);

        if (startTime == null || startTime.isEmpty()){
            // 开始时间精确到小时
            startTime = LocalDateTime.now().withMinute(0).format(format);
        }

//        fileRoundUnit = props.get(FILE_ROUNDUNIT);

        if (filename == null || filename.isEmpty()){
            filePrefix = props.get(FILE_PREFIX_CONFIG);
            fileSuffix = props.get(FILE_SUFFIX_CONFIG);
            filename = path + File.separator + filePrefix + startTime + fileSuffix;
        }

        topic = props.get(TOPIC_CONFIG);
        if (filename == null || filename.isEmpty())
            throw new ConnectException("FileStreamSource configuration must include 'file or (file.prefix & file.suffix & file.date)' setting");
        if (path == null || path.isEmpty())
            throw new ConnectException("FileStreamSource configuration must include 'path' setting");
        if (topic == null || topic.isEmpty())
            throw new ConnectException("FileStreamSource configuration must include 'topic' setting");
        if (topic.contains(","))
            throw new ConnectException("FileStreamSource should only have a single topic when used as a source.");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FileStreamSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        // Only one input stream makes sense.
        Map<String, String> config = new HashMap<>();
        config.put(PATH_CONFIG, path);

        if (filePrefix != null && !(filePrefix.isEmpty()))
            config.put(FILE_PREFIX_CONFIG, filePrefix);
        if (fileSuffix != null && !(fileSuffix.isEmpty()))
            config.put(FILE_PREFIX_CONFIG, fileSuffix);
        if (fileDateFormatter != null && !(fileDateFormatter.isEmpty()))
            config.put(FILE_DATE_CONFIG, fileDateFormatter);

//        config.put(FILE_ROUNDUNIT, fileRoundUnit);
        config.put(START_TIME, startTime);
        config.put(FILE_CONFIG, filename);
        config.put(TOPIC_CONFIG, topic);
        configs.add(config);
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do since FileStreamSource has no background monitoring.
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
