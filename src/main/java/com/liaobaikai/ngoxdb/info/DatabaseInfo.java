package com.liaobaikai.ngoxdb.info;

import lombok.Getter;
import lombok.Setter;

/**
 * 数据库信息
 *
 * @author baikai.liao
 * @Time 2021-02-03 23:19:38
 */
@Setter
@Getter
public class DatabaseInfo {

    /**
     * 数据库名
     */
    private String tableCat;

    /**
     * 模式，可能为NULL, 登录用户？？？？
     */
    private String tableSchem;

    /**
     * 数据库默认字符集
     */
    private String charsetName;

    /**
     * 最大长度
     */
    private int maxLen;

}
