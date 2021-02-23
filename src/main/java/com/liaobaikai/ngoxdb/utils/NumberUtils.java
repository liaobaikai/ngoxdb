package com.liaobaikai.ngoxdb.utils;

/**
 * @author baikai.liao
 * @Time 2021-01-23 19:59:17
 */
public class NumberUtils {

    public static int toInt(Object value) {
        if (value instanceof Integer) {
            return (int) value;
        } else if (value instanceof Long) {
            return ((Long) value).intValue();
        }
        return Integer.parseInt(String.valueOf(value));
    }

    public static long toLong(Object value) {
        if (value instanceof Integer) {
            return ((Integer) value).longValue();
        } else if (value instanceof Long) {
            return (long) value;
        }
        return value == null ? 0 : Long.parseLong(String.valueOf(value));
    }

}
