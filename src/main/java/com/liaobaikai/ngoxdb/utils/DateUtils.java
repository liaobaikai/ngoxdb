package com.liaobaikai.ngoxdb.utils;

import java.sql.Timestamp;

/**
 * @author baikai.liao
 * @Time 2021-01-23 16:48:57
 */
public class DateUtils {

    /**
     * 将毫秒格式化为可视化时间
     *
     * @param mills 毫秒
     * @return 如 1100 00:00:01 100
     */
    public static String format(long mills) {
        // 毫秒
        long m = (mills % 1000);
        long s = (mills / 1000) % 60;
        long M = (mills / 60000) % 60;
        long h = (mills / 3600000) % 60;

        return String.format("%02d:%02d:%02d %03d", h, M, s, m);
    }

    /**
     * 去掉毫秒值
     *
     * @param src {@link java.sql.Timestamp}
     * @return {@link java.sql.Timestamp}
     */
    public static Timestamp stripMills(Timestamp src) {
        if (src.getNanos() > 0) {
            return new Timestamp(src.getTime() - src.getNanos() / 1000000);
        }
        return src;
    }

    public static boolean isDateValue(Class<?> inValueType) {
        return java.util.Date.class.isAssignableFrom(inValueType)
                || java.sql.Date.class.isAssignableFrom(inValueType)
                || java.sql.Time.class.isAssignableFrom(inValueType)
                || java.sql.Timestamp.class.isAssignableFrom(inValueType);
    }

    public static void main(String[] args) {

        format(1100);

    }
}
