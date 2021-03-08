package com.liaobaikai.ngoxdb.bean.rs;

import lombok.Getter;
import lombok.Setter;

/**
 * 主键信息
 *
 * @author baikai.liao
 * @Time 2021-01-27 15:33:14
 */
@Setter
@Getter
public class PrimaryKey extends TableBase {

    /**
     * 列名
     */
    private String columnName;

    /**
     * 序号
     */
    private short keySeq;

    /**
     * 主键名称
     */
    private String pkName;
}
