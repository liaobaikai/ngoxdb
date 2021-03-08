package com.liaobaikai.ngoxdb.core.entity;

import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

/**
 * 用于保存需要创建的索引，外键等信息
 *
 * @author baikai.liao
 * @Time 2021-01-18 18:02:10
 */
@Setter
@Getter
public class SlaveMetaDataEntity {

    public static final String TABLE_NAME = "ngoxdb_slave_metadata";

    /**
     * {@link java.util.UUID}
     */
    private String metadataId = UUID.randomUUID().toString();

    /**
     * 表名
     */
    private String tableName;

    public static final String TYPE_PRIMARY_KEY = "PRIMARY KEY";
    public static final String TYPE_UNIQUE_INDEX = "UNIQUE KEY";
    public static final String TYPE_FOREIGN_KEY = "FOREIGN KEY";

    public static final String TYPE_INDEX = "INDEX";
    public static final String TYPE_FULLTEXT_INDEX = "FULLTEXT INDEX";
    public static final String TYPE_SPATIAL_INDEX = "SPATIAL INDEX";

    public static final String TYPE_CHECK = "CHECK";
    public static final String TYPE_COMMENT = "COMMENT";
    public static final String TYPE_OTHER = "OTHER";

    /**
     * 类型
     */
    private String type;

    /**
     * 语句
     */
    private String statement;

    /**
     * 是否应用
     */
    private Integer isUsed = NOT_USED;
    public static final int USED = 1;
    public static final int NOT_USED = 0;

    public SlaveMetaDataEntity() {
    }

    public SlaveMetaDataEntity(String tableName, String type, String statement) {
        this.tableName = tableName;
        this.type = type;
        this.statement = statement;
    }
}
