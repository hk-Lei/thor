package com.emar.kafka.utils;

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

    public static String FORMAT1 = "yyyyMMddHHmm";
    public static String FORMAT2 = "yyyyMMddHH";
    public static String FORMAT3 = "yyyyMMdd";

    public static LocalDateTime getLocalDateTime(Instant time) {
        return LocalDateTime.ofInstant(time, ZoneId.of("Asia/Shanghai"));
    }

    public static DateTimeFormatter getFormatter(String time){
        Pattern p1 = Pattern.compile("\\d{12}");
        Pattern p2 = Pattern.compile("\\d{10}");
        Pattern p3 = Pattern.compile("\\d{8}");

        boolean b1 = p1.matcher(time).matches();
        boolean b2 = p2.matcher(time).matches();
        boolean b3 = p3.matcher(time).matches();

        return b1?DateTimeFormatter.ofPattern(FORMAT1):(
                b2?DateTimeFormatter.ofPattern(FORMAT2):(
                        b3?DateTimeFormatter.ofPattern(FORMAT3) : null));
    }
}
