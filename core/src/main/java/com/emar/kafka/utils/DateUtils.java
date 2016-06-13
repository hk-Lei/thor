package com.emar.kafka.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * @Author moxingxing
 * @Date 2016/6/13
 */
public class DateUtils {

    public static String FORMAT1 = "yyyyMMddHHmm";
    public static String FORMAT2 = "yyyyMMddHH";
    public static String FORMAT3 = "yyyyMMdd";

    public static LocalDateTime getLocalDateTime(Instant time) {
        return LocalDateTime.ofInstant(time, ZoneId.of("Asia/Shanghai"));
    }

    public static String getFormatter(){

    }
}
