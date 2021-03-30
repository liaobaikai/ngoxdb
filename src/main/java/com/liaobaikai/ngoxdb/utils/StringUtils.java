package com.liaobaikai.ngoxdb.utils;

import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author baikai.liao
 * @Time 2021-01-27 17:00:22
 */
public class StringUtils extends org.apache.commons.lang3.StringUtils {

    /**
     * 需要转义的特殊字符
     */
    private static final String[] REGEX_CHARS = new String[]{"\\*", "\\+", "\\?", "\\|", "\\{", "\\[", "\\(", "\\)", "\\^", "\\$", "\\."};

    /**
     * url参数转map，如 name=baikai&age=28 => {name: baikai, age: 28}
     *
     * @param urlParam 参数
     * @return map
     */
    public static LinkedHashMap<String, String> url2Map(String urlParam) {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        String[] param = urlParam.split("&");
        for (String keyValue : param) {
            String[] pair = keyValue.split("=");
            if (pair.length == 2) {
                map.put(pair[0], pair[1]);
            } else {
                map.put(pair[0], "");
            }
        }
        return map;
    }

    /**
     * map转url参数 如 {name: baikai, age: 28} => name=baikai&age=28
     *
     * @param map map
     * @return url参数
     */
    public static String map2Url(Map<String, String> map) {

        StringBuilder urlParamBuilder = new StringBuilder();
        if (map != null && map.size() > 0) {
            map.forEach((key, value) -> urlParamBuilder.append(key).append("=").append(value).append("&"));
            urlParamBuilder.delete(urlParamBuilder.length() - 1, urlParamBuilder.length());
        }

        return urlParamBuilder.toString();
    }


    /**
     * 将字符串转义为正则表达式
     *
     * @param regex
     * @return
     */
    public static String escape(String regex) {
        // 对应的字符进行转义
        for (String chr : REGEX_CHARS) {
            regex = regex.replaceAll(chr, "\\" + chr);
        }
        return regex;
    }

    /**
     * 将clob转换为字符串
     *
     * @param clob clob对象
     * @return 字符串
     * @throws SQLException sql错误
     */
    public static String clob2String(Clob clob) throws SQLException {
        // Clob & NClob
        Reader reader = clob.getCharacterStream();
        StringBuilder stringBuilder = new StringBuilder();
        char[] buf = new char[1024];
        int readCount = 0;
        while (true) {
            try {
                readCount = reader.read(buf);
                if (readCount == -1) break;
            } catch (IOException e) {
                e.printStackTrace();
            }
            stringBuilder.append(buf, 0, readCount);

        }
        return stringBuilder.toString();
    }

    public static boolean isStringValue(Class<?> inValueType) {
        // Consider any CharSequence (including StringBuffer and StringBuilder) as a String.
        return (CharSequence.class.isAssignableFrom(inValueType) ||
                StringWriter.class.isAssignableFrom(inValueType));
    }

    public static String join(char sp, String[] values) {
        StringBuilder sBuilder = new StringBuilder();
        for (int i = 0, len = values.length; i < len; i++) {
            sBuilder.append(" ").append(values[i]);
            if (i != len - 1) {
                sBuilder.append(sp);
            }
        }
        return sBuilder.toString();
    }

    public static String[] toArray(List<String> values) {
        String[] result = new String[values.size()];
        for (int i = 0, len = result.length; i < len; i++) {
            result[i] = values.get(i);
        }
        return result;
    }


}
