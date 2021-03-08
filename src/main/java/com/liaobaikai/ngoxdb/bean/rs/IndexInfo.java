package com.liaobaikai.ngoxdb.bean.rs;

import lombok.Getter;
import lombok.Setter;

/**
 * 索引信息
 *
 * @author baikai.liao
 * @Time 2021-01-27 15:54:25
 */
@Setter
@Getter
public class IndexInfo extends TableBase {

    /**
     * 是否唯一
     */
    private boolean nonUnique;

    /**
     * 索引修饰词，可能为NULL
     * index catalog
     */
    private String indexQualifier;

    /**
     * 索引名称，可能为NULL
     */
    private String indexName;

    /**
     * 索引类型
     * {@link java.sql.DatabaseMetaData}
     * 0: 统计信息
     * 1: 集群索引
     * 2: 哈希索引
     * 3: 其他索引
     */
    private short type;

    /**
     * 列的序号
     */
    private short ordinalPosition;

    /**
     * 列名
     */
    private String columnName;

    /**
     * 排序 A：顺序，D：倒序
     */
    private String ascOrDesc;

    /**
     * 索引基数，当type为0的时候，显示表的行数。其他类型的话，显示唯一值
     */
    private long cardinality;

    /**
     * 页数
     */
    private long pages;

    /**
     * 过滤条件，可能为NULL
     */
    private String filterCondition;

    /**
     * 顺序
     */
    public static final String ORDER_ASC = "A";
    /**
     * 倒序
     */
    public static final String ORDER_DESC = "D";
}
