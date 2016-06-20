package com.emar.kafka.utils;

/**
 * @author moxingxing
 * @Date 2016/6/20
 */
public class StringUtils {
    public static boolean isNotBlank(String string){
        return !isBlank(string);
    }

    public static boolean isBlank(String string){
        return (string == null || string.isEmpty() || string.trim().isEmpty()) || "null".equals(string);
    }

    public static boolean isNotEmpty(String string){
        return !isEmpty(string);
    }

    public static boolean isEmpty(String string){
        return (string == null || string.isEmpty());
    }

    public static boolean checkBothIsBlank(String str1, String str2){
        return isBlank(str1) && isBlank(str2);
    }
}
