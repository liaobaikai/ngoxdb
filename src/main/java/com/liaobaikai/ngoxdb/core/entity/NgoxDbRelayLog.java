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
public class NgoxDbRelayLog {

    public static final String TABLE_NAME = "ngoxdb_relay_log";

    /**
     * {@link java.util.UUID}
     */
    private String logId = UUID.randomUUID().toString();

    /**
     * 表名
     */
    private String logTableName;

    public static final String TYPE_PRIMARY_KEY = "PRIMARY KEY";

    public static final String TYPE_UNIQUE_INDEX = "UNIQUE KEY";

    public static final String TYPE_FOREIGN_KEY = "FOREIGN KEY";

    public static final String TYPE_CREATE_TABLE = "CREATE TABLE";

    public static final String TYPE_INDEX = "INDEX";

    public static final String TYPE_FULLTEXT_INDEX = "FULLTEXT INDEX";

    public static final String TYPE_SPATIAL_INDEX = "SPATIAL INDEX";

    public static final String TYPE_CHECK = "CHECK";

    public static final String TYPE_COMMENT = "COMMENT";

    public static final String TYPE_OTHER = "OTHER";

    /**
     * 类型
     */
    private String logType;

    /**
     * 语句
     */
    private String logText;

    /**
     * 是否应用
     */
    private int logUsed;

    public static final int USED = 1;
    public static final int NOT_USED = 0;

    public NgoxDbRelayLog() {
    }

    public NgoxDbRelayLog(String logTableName, String logType, String logText) {
        this.logTableName = logTableName;
        this.logType = logType;
        this.logText = logText;
    }
}
