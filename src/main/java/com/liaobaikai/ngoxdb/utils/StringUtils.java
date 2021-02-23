package com.liaobaikai.ngoxdb.utils;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author baikai.liao
 * @Time 2021-01-27 17:00:22
 */
public class StringUtils extends org.apache.commons.lang3.StringUtils{

    /**
     * 需要转义的特殊字符
     */
    private static final String[] REGEX_CHARS = new String[]{"\\*", "\\+", "\\?", "\\|", "\\{", "\\[", "\\(", "\\)", "\\^", "\\$", "\\."};

    /**
     * url参数转map，如 name=baikai&age=28 => {name: baikai, age: 28}
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
     * @param map map
     * @return url参数
     */
    public static String map2Url(Map<String, String> map){

        StringBuilder urlParamBuilder = new StringBuilder();
        if(map != null && map.size() > 0) {
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
}
