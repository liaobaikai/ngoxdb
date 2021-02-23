package com.liaobaikai.ngoxdb.enums;

/**
 * 索引类型
 *
 * @author baikai.liao
 * @Time 2021-01-29 22:31:49
 */
public enum IndexTypeEnum {

    BTREE,              // b-tree索引
    UNIQUE,             // 唯一索引
    HASH,               // 哈希索引
    BITMAP,             // 位图索引
    FULLTEXT,           // 全文索引
    SPATIAL,            // 空间索引
    FUNCTION_BASED,     // 函数索引

}
