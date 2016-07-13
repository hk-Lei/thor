package com.emar.kafka.connect;

import com.emar.kafka.utils.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

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
    public static final String START_TIME = "start.time";
    public static final String IGNORE_OFFSET = "ignore.offset";
    public static final String IS_RECURSIVE_DIR = "is.recusive.dir";
    public static final String RM_HISTORY_FILES_INTERVAL_HOURS = "rm.history.files.interval.hours";
    public static final String INTERCEPTOR_SCHEME = "interceptor.scheme";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(PATH_CONFIG, Type.STRING, Importance.HIGH, "Source path.")
            .define(START_TIME, Type.STRING, Importance.LOW, "Source start time.")
            .define(IS_RECURSIVE_DIR, Type.STRING, Importance.LOW, "Source is recursive dir.")
            .define(IGNORE_OFFSET, Type.STRING, Importance.LOW, "Source ignore offset.")
            .define(RM_HISTORY_FILES_INTERVAL_HOURS, Type.STRING, Importance.LOW, "Source rm.history.files.interval.hours.")
            .define(INTERCEPTOR_SCHEME, Type.STRING, Importance.LOW, "Source interceptor scheme.")
            .define(FILE_PREFIX_CONFIG, Type.STRING, Importance.HIGH, "Source filename prefix.")
            .define(FILE_SUFFIX_CONFIG, Type.STRING, Importance.HIGH, "Source filename suffix.")
            .define(TOPIC_CONFIG, Type.STRING, Importance.HIGH, "The topic to publish data to");

    private String path;
    private String startTime;
    private String ignoreOffset;
    private String isRecursive;
    private String rmIntervalHours;
    private String filePrefix;
    private String fileSuffix;
    private String topic;
    private String scheme;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        path = props.get(PATH_CONFIG);
        startTime = props.get(START_TIME);
        ignoreOffset = props.get(IGNORE_OFFSET);
        isRecursive = props.get(IS_RECURSIVE_DIR);
        rmIntervalHours = props.get(RM_HISTORY_FILES_INTERVAL_HOURS);
        filePrefix = props.get(FILE_PREFIX_CONFIG);
        fileSuffix = props.get(FILE_SUFFIX_CONFIG);
        scheme = props.get(INTERCEPTOR_SCHEME);

        topic = props.get(TOPIC_CONFIG);
        if (StringUtils.checkBothIsBlank(filePrefix, fileSuffix))
            throw new ConnectException("FileStreamSource configuration must include 'file.prefix or file.suffix or both' setting");
        if (StringUtils.isBlank(path))
            throw new ConnectException("FileStreamSource configuration must include 'path' setting");
        if (StringUtils.isBlank(topic))
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
        if (ignoreOffset != null)
            config.put(IGNORE_OFFSET, ignoreOffset);
        if (isRecursive != null)
            config.put(IS_RECURSIVE_DIR, isRecursive);
        if (rmIntervalHours != null)
            config.put(RM_HISTORY_FILES_INTERVAL_HOURS, rmIntervalHours);
        if (startTime != null)
            config.put(START_TIME, startTime);
        if (filePrefix != null)
            config.put(FILE_PREFIX_CONFIG, filePrefix);
        if (fileSuffix != null)
            config.put(FILE_SUFFIX_CONFIG, fileSuffix);
        config.put(TOPIC_CONFIG, topic);
        if (scheme !=null)
            config.put(INTERCEPTOR_SCHEME, scheme);
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
