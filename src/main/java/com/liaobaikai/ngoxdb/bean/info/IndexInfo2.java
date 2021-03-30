package com.liaobaikai.ngoxdb.bean.info;

import com.liaobaikai.ngoxdb.bean.rs.IndexInfo;
import lombok.Getter;
import lombok.Setter;

import java.sql.DatabaseMetaData;

/**
 * @author baikai.liao
 * @Time 2021-01-29 19:17:25
 */
@Setter
@Getter
public class IndexInfo2 extends IndexInfo {

    /**
     * 统计信息
     */
    public static final short tableIndexStatistic = DatabaseMetaData.tableIndexStatistic;

    /**
     * 聚集索引
     */
    public static final short tableIndexClustered = DatabaseMetaData.tableIndexClustered;

    /**
     * hash索引
     */
    public static final short tableIndexHashed = DatabaseMetaData.tableIndexHashed;

    /**
     * 其他索引
     */
    public static final short tableIndexOther = DatabaseMetaData.tableIndexOther;

    /**
     * 全文索引
     */
    public static final short tableIndexFullText = 10;

    /**
     * 空间索引
     */
    public static final short tableIndexSpatial = 11;

    /**
     * 位图索引
     */
    public static final short tableIndexBitmap = 12;

    /**
     * 分区索引
     */
    public static final short tableIndexPartitioned = 13;

    /**
     * 函数索引
     */
    public static final short tableIndexFunction = 14;

    /**
     * 域索引
     */
    public static final short tableIndexDomain = 15;

    /**
     * 是否为统计信息
     *
     * @return boolean
     */
    public boolean isTableIndexStatistic() {
        return this.getType() == tableIndexStatistic;
    }

    /**
     * 获取排序的名称
     *
     * @return ASC 、 DESC
     */
    public String getOrder() {
        if (this.getAscOrDesc() == null) {
            return "";
        }

        switch (this.getAscOrDesc()) {
            case ORDER_ASC:
                return "ASC";
            case ORDER_DESC:
                return "DESC";
            default:
                return "";
        }
    }
}
