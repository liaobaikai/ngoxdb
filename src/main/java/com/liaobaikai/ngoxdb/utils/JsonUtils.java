package com.liaobaikai.ngoxdb.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import java.util.List;

/**
 * JSON 工具
 *
 * @author baikai.liao
 * @Time 2021-03-17 22:31:27
 */
public class JsonUtils {

    public static <T> List<T> toJavaObject(Object obj) {
        return ((JSON) JSON.toJSON(obj)).toJavaObject(new TypeReference<List<T>>() {
        });
    }

}
