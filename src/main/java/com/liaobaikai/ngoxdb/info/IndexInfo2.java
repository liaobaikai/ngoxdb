package com.liaobaikai.ngoxdb.info;

import com.liaobaikai.ngoxdb.rs.IndexInfo;
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
     * 索引类型，每个数据库的自己的描述
     */
    private String indexTypeDesc;

    /**
     * 是否为统计信息
     * @return boolean
     */
    public boolean isTableIndexStatistic(){
        return this.getType() == DatabaseMetaData.tableIndexStatistic;
    }

    /**
     * 获取排序的名称
     * @return ASC 、 DESC
     */
    public String getOrder(){
        if(this.getAscOrDesc() == null){
            return "";
        }

        switch (this.getAscOrDesc()){
            case ORDER_ASC:
                return "ASC";
            case ORDER_DESC:
                return "DESC";
            default:
                return "";
        }
    }
}
