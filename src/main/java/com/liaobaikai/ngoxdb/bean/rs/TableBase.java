package com.liaobaikai.ngoxdb.bean.rs;

import lombok.Getter;
import lombok.Setter;

/**
 * @author baikai.liao
 * @Time 2021-01-28 12:06:53
 */
@Setter
@Getter
public class TableBase {

    /**
     * 数据库名
     */
    private String tableCat;

    /**
     * 模式，可能为NULL, 登录用户？？？？
     */
    private String tableSchem;

    /**
     * 表名
     */
    private String tableName;
}
