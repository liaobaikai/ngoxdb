package com.liaobaikai.ngoxdb.utils;

import java.sql.Types;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author baikai.liao
 * @Time 2021-02-20 23:10:33
 */
public final class CommonUtils {

    public static final String BANNER_NAME = "NGOXDB";

    public static Object[] getMapValues(Map<String, Object> map) {
        Object[] values = new Object[map.size()];
        int j = 0;
        for (Map.Entry<String, Object> en : map.entrySet()) {
            values[j++] = en.getValue();
            System.out.println(en.getValue().getClass());
        }
        return values;
    }

    // public static void log2(DatabaseConfig databaseConfig, String msg, Object... args) {
    //     log2(true, null, databaseConfig, msg, args);
    // }
    //
    // public static void log2(boolean newline, Counter counter, DatabaseConfig databaseConfig, String msg, Object... args) {
    //     for (Object arg : args) {
    //         msg = msg.replaceFirst("\\{}", arg.toString());
    //     }
    //     System.out.print(BANNER_NAME + " [" + databaseConfig.getName() + (counter == null ? "" : ", " + counter.getPercentage() + ", " + counter.getCount() + "/" + counter.getSum()) + "]<<< " + msg + (newline ? "\n" : ""));
    // }
    //
    // public static void warn(DatabaseConfig databaseConfig, String msg, Object... args) {
    //     for (Object arg : args) {
    //         msg = msg.replaceFirst("\\{}", arg.toString());
    //     }
    //     System.out.println(BANNER_NAME + " [" + databaseConfig.getName() + "]<<< " + "\033[33m" + msg + "\033[m");
    // }
    //
    // public static void skip(DatabaseConfig databaseConfig, String msg, Object... args) {
    //     for (Object arg : args) {
    //         msg = msg.replaceFirst("\\{}", arg.toString());
    //     }
    //     System.out.println(BANNER_NAME + " [" + databaseConfig.getName() + "]<<< " + msg + " Skip.");
    // }
    //
    // public static void log(Counter counter, DatabaseConfig databaseConfig, String msg, Object... args) {
    //     for (Object arg : args) {
    //         msg = msg.replaceFirst("\\{}", arg.toString());
    //     }
    //     System.out.println(BANNER_NAME + " [" + databaseConfig.getName() + (counter == null ? "" : ", " + counter.getPercentage() + ", " + counter.getCount() + "/" + counter.getSum()) + "]>>> " + "\033[32m" + msg + "\033[m");
    // }

    /**
     * 是否为数字类型的jdbc type
     *
     * @param jdbcType
     * @return
     */
    public static boolean isJdbcDecimal(int jdbcType) {
        switch (jdbcType) {
            case Types.BIT:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return true;
        }
        return false;
    }

    /**
     * 获取map中的第一个值
     *
     * @param map
     * @return
     */
    public static Object getFirstValue(Map<String, Object> map) {
        for (Map.Entry<String, Object> en : map.entrySet()) {
            return en.getValue();
        }
        return null;
    }

    public static TreeMap<String, Long> mapOfConvert(Map<String, AtomicLong> map) {

        TreeMap<String, Long> result = new TreeMap<>();

        if (map == null) {
            return null;
        }

        for (Map.Entry<String, AtomicLong> en : map.entrySet()) {
            result.put(en.getKey(), en.getValue().get());
        }

        return result;
    }

}
