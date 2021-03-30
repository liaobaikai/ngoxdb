package com.liaobaikai.ngoxdb.bean.info;

import com.liaobaikai.ngoxdb.bean.rs.ExportedKey;
import com.liaobaikai.ngoxdb.bean.rs.ImportedKey;
import com.liaobaikai.ngoxdb.bean.rs.PrimaryKey;
import com.liaobaikai.ngoxdb.bean.rs.Table;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 表的其他信息，通过查询数据库扩展
 *
 * @author baikai.liao
 * @Time 2021-01-27 22:53:50
 */
@Setter
@Getter
public class TableInfo extends Table {

    /**
     * 数据库厂家的名称
     */
    private String databaseVendorName;

    /**
     * 自增长的值，不支持为null，下一个值
     */
    private Long autoIncrement;

    /**
     * 默认排序规则，不支持为null
     */
    private String tableCollation;

    /**
     * 选项
     */
    private String createOptions;

    /**
     * 分区信息
     */
    private List<PartitionInfo> partitions;

    /**
     * 列信息
     */
    private List<ColumnInfo> columns;

    /**
     * 主键信息
     */
    private List<PrimaryKey> primaryKeys;

    /**
     * 索引信息
     */
    private List<IndexInfo2> indexInfo;

    /**
     * 本表引用其他表的键（外键）
     */
    private List<ImportedKey> importedKeys;

    /**
     * 其他表引用本表的键（外键）
     */
    private List<ExportedKey> exportedKeys;

    /**
     * 约束的信息
     */
    private List<ConstraintInfo> constraintInfo;

    /**
     * 唯一约束、主键约束
     */
    private List<String> uniqueKeys = new ArrayList<>();

    /**
     * 预处理过的插入语句
     */
    private Map<String, String> mapOfPreparedInsertSql = new HashMap<>();

    /**
     * 是否含有自增列
     */
    private boolean hasAutoIdentity;

}
