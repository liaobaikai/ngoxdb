package com.liaobaikai.ngoxdb.bean;

import lombok.Getter;
import lombok.Setter;

import java.util.Date;

/**
 * 比较结果
 *
 * @author baikai.liao
 * @Time 2021-03-04 15:09:38
 */
@Setter
@Getter
public class ComparisonResult {

    /**
     * 开始时间
     */
    private Date beginTime = new Date();

    /**
     * 错误数
     */
    private int errors;

    /**
     * 不同的数量
     */
    private int diffs;

    /**
     * 检查的行数
     */
    private long rows;

    /**
     * 不同的行数
     */
    private int diffRows;

    /**
     * 页数
     */
    private int pages;

    /**
     * 跳过数量
     */
    private long skipped;

    /**
     * 完成时间
     */
    private Date finishTime;

    /**
     * 从数据库名
     */
    private String slaveName;

    /**
     * 表名
     */
    private String tableName;

}
