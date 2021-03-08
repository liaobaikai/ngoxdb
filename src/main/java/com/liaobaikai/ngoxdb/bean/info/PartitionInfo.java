package com.liaobaikai.ngoxdb.bean.info;

import lombok.Getter;
import lombok.Setter;

/**
 * 分区信息
 *
 * @author baikai.liao
 * @Time 2021-01-27 23:42:08
 */
@Setter
@Getter
public class PartitionInfo {

    private String tableName;

    /**
     * 分区
     */
    private String partitionOrdinalPosition;
    private String partitionMethod;
    private String partitionExpression;
    private String partitionName;
    private String partitionDescription;

    /**
     * 子分区
     */
    private String subpartitionOrdinalPosition;
    private String subpartitionMethod;
    private String subpartitionExpression;
    private String subpartitionName;

    // 分区注释
    private String partitionComment;

}
