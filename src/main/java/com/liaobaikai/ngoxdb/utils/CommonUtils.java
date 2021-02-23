package com.liaobaikai.ngoxdb.utils;

import java.util.Map;

/**
 * @author baikai.liao
 * @Time 2021-02-20 23:10:33
 */
public final class CommonUtils {

    public static Object[] getMapValues(Map<String, Object> map){
        Object[] values = new Object[map.size()];
        int j = 0;
        for (Map.Entry<String, Object> en : map.entrySet()) {
            values[j++] = en.getValue();
        }
        return values;
    }
}
