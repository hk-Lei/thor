package com.emar.kafka.utils;

import com.emar.kafka.offset.OffsetValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

/**
 * @Author moxingxing
 * @Date 2016/6/13
 */
public class DateUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DateUtils.class);
    public static final String FORMAT1 = "yyyyMMddHHmm";
    public static final String FORMAT2 = "yyyyMMddHH";
    public static final String FORMAT3 = "yyyyMMdd";

    private static final String offsetPattern = "yyyy-MM-dd HH:mm:ss.SSS";

    public static LocalDateTime getLocalDateTime(Instant time) {
        return LocalDateTime.ofInstant(time, ZoneId.of("Asia/Shanghai"));
    }

    public static DateTimeFormatter getFormatter(String time){
        try {
            Pattern p1 = Pattern.compile("\\d{12}");
            Pattern p2 = Pattern.compile("\\d{10}");
            Pattern p3 = Pattern.compile("\\d{8}");

            boolean b1 = p1.matcher(time).matches();
            boolean b2 = p2.matcher(time).matches();
            boolean b3 = p3.matcher(time).matches();

            return b1 ? DateTimeFormatter.ofPattern(FORMAT1) : (
                    b2 ? DateTimeFormatter.ofPattern(FORMAT2) : (
                            b3 ? DateTimeFormatter.ofPattern(FORMAT3) : null));
        } catch (Exception e) {
            return null;
        }
    }

    public static LocalDateTime getStart(String startTime) {
        DateTimeFormatter format = DateUtils.getFormatter(startTime);
        if (startTime == null || startTime.isEmpty() || format == null) {
            LocalDateTime time = LocalDateTime.now().withSecond(0).minusMinutes(10);
            // 开始时间
            LOG.warn("start.time 为空或者非法(格式必须为 yyyyMMddHHmm 或 yyyyMMddHH 或者 yyyyMMdd) ");
            LOG.warn("start.time 设置为当前时间(精确到分钟) - 10 分钟 ：{}", time);
            return time;
        } else
            return LocalDateTime.parse(startTime, format);
    }

    public static LocalDateTime getOffsetLastModifyTime(OffsetValue offsetValue){
        DateTimeFormatter format = DateTimeFormatter.ofPattern(offsetPattern);
        if (offsetValue != null) {
            String lastModifyTime = offsetValue.getLastModifyTime();
            return LocalDateTime.parse(lastModifyTime, format);
        } else {
            return null;
        }
    }

    public static String getOffsetLastModifyTime(LocalDateTime dateTime){
        DateTimeFormatter format = DateTimeFormatter.ofPattern(offsetPattern);
        return format.format(dateTime);
    }
}
