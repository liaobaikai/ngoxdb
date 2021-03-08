package com.liaobaikai.ngoxdb.core.udf;

import net.ucanaccess.converters.TypesMap;
import net.ucanaccess.ext.FunctionType;

/**
 * Access 数据库 扩展的 函数
 * @author baikai.liao
 * @Time 2021-03-08 15:36:02
 */
public class AccessDatabaseFunctions {

    /**
     * 字符串转byte
     * @param s 字符串
     * @return byte[]
     */
    @FunctionType(functionName = "TO_BYTE", argumentTypes = { TypesMap.AccessType.TEXT }, returnType = TypesMap.AccessType.BYTE)
    public static byte[] toByte(String s) {
        return s.getBytes();
    }

}
