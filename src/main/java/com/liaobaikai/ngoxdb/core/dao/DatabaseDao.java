package com.liaobaikai.ngoxdb.core.dao;

import com.liaobaikai.ngoxdb.bean.info.*;
import com.liaobaikai.ngoxdb.bean.rs.*;
import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
import com.liaobaikai.ngoxdb.core.entity.SlaveMetaDataEntity;
import org.slf4j.Logger;

import java.sql.Types;
import java.util.List;

/**
 * @author baikai.liao
 * @Time 2021-01-23 00:43:10
 */
public interface DatabaseDao {

    /**
     * 字符串类型
     */
    int[] STRING_JDBC_TYPES = {
            Types.CHAR,
            Types.VARCHAR,
            Types.LONGVARCHAR,
            Types.BINARY,
            Types.VARBINARY,
            Types.LONGVARBINARY,
            Types.BLOB,
            Types.CLOB,
            Types.ROWID,
            Types.NCHAR,
            Types.NVARCHAR,
            Types.LONGNVARCHAR,
            Types.NCLOB,
            Types.SQLXML,
            microsoft.sql.Types.GUID,
            microsoft.sql.Types.SQL_VARIANT
    };

    /**
     * 数字类型
     */
    int[] NUMBER_JDBC_TYPES = {
            Types.BIT,
            Types.TINYINT,
            Types.SMALLINT,
            Types.INTEGER,
            Types.BIGINT,
            Types.FLOAT,
            Types.REAL,
            Types.DOUBLE,
            Types.NUMERIC,
            Types.DECIMAL,
            Types.BOOLEAN,
            microsoft.sql.Types.MONEY,
            microsoft.sql.Types.SMALLMONEY,
    };

    /**
     * 日期类型
     */
    int[] DATE_TIME_JDBC_TYPES = {
            Types.DATE,
            Types.TIME,
            Types.TIMESTAMP,
            Types.TIME_WITH_TIMEZONE,
            Types.TIMESTAMP_WITH_TIMEZONE,
            microsoft.sql.Types.DATETIME,
            microsoft.sql.Types.DATETIMEOFFSET,
            microsoft.sql.Types.SMALLDATETIME,
    };

    Logger getLogger();

    /**
     * 获取数据库名称
     *
     * @return String
     */
    String getCatalog();

    /**
     * 获取数据库的信息
     *
     * @return DatabaseInfo
     */
    DatabaseInfo getDatabaseInfo();

    /**
     * 检索此数据库允许列名称的最大字符数。
     * @return 列名允许的最大字符数; 零的结果意味着没有限制或限制是不知道的
     */
    int getMaxColumnNameLength();

    /**
     * 检索数据库在SQL语句中允许的最大字符数。
     * @return SQL语句允许的最大字符数; 零的结果意味着没有限制或限制是不知道的
     */
    int getMaxStatementLength();

    /**
     * 检索方法 getMaxRowSize的返回值是否包含SQL数据类型 LONGVARCHAR 和 LONGVARBINARY 。
     * @return true如果是的话; false否则
     */
    boolean doesMaxRowSizeIncludeBlobs();

    /**
     * 默认的模式通配符
     *
     * @return 默认为 %
     */
    default String getSchemaPattern() {
        return "%";
    }

    /**
     * 获取JdbcTemplate2对象
     *
     * @return JdbcTemplate2
     */
    JdbcTemplate2 getJdbcTemplate();

    /**
     * 创建视图的定义
     *
     * @param viewName 视图名称
     * @return SQL
     */
    default String getCreateViewDefinition(String viewName) {
        return null;
    }

    /**
     * 获取表信息
     *
     * @param tableName 表名
     * @return 查询到的表
     */
    List<TableInfo> getTables(String... tableName);

    /**
     * 获取表的列信息
     *
     * @param tableName 表名
     * @return 查询到的列
     */
    List<ColumnInfo> getColumns(String... tableName);

    /**
     * 获取表的主键信息
     *
     * @param schema    模式
     * @param tableName 表名
     * @return 主键信息
     */
    List<PrimaryKey> getPrimaryKeys(String schema, String tableName);

    /**
     * 获取表的外键信息
     *
     * @param schema    模式
     * @param tableName 表名
     * @return 外键信息
     */
    List<ImportedKey> getImportedKeys(String schema, String tableName);

    /**
     * 获取表的外键引用信息，那个表引用了tableName表
     *
     * @param schema    模式
     * @param tableName 表名
     * @return 外键引用信息
     */
    List<ExportedKey> getExportedKeys(String schema, String tableName);

    /**
     * Retrieves a description of a table's columns that are automatically updated when any value in a row is updated. They are unordered.
     *
     * @param schema    模式
     * @param tableName 表名
     * @return List<VersionColumn>
     */
    List<VersionColumn> getVersionColumns(String schema, String tableName);

    /**
     * 用户定义类型（UDT）的给定类型的给定属性的描述。
     *
     * @param typeName 类型名称
     * @return List<Attribute>
     */
    List<Attribute> getAttributes(String... typeName);

    /**
     * 获取表的索引信息
     *
     * @param schema    模式
     * @param tableName 表名
     * @return 索引信息
     */
    List<IndexInfo2> getIndexInfo(String schema, String tableName);

    /**
     * 主键默认就是索引，因此需要删除主键的索引信息。在创建表的同时不需要再次创建一个唯一的索引。
     */
    void removePrimaryIndex(List<IndexInfo2> indexInfoList);

    /**
     * 查询表的个数
     *
     * @param schema    模式
     * @param tableName 表名
     * @return 数量
     */
    int getTableCount(String schema, String tableName);

    /**
     * 获取约束信息
     *
     * @param tableName 表
     * @return List<CheckConstraint>
     */
    List<ConstraintInfo> getConstraintInfo(String... tableName);

    /**
     * 获取表的记录的行数
     *
     * @param tableName 表名
     * @return 行数
     */
    long getTableRowCount(String schema, String tableName);

    /**
     * 删除表
     *
     * @param tableName 表名
     */
    void dropTable(String tableName);

    /**
     * 删除表数据
     *
     * @param tableName 表名
     */
    void deleteTable(String tableName);

    /**
     * 截断表
     *
     * @param tableName 表名
     */
    void truncateTable(String tableName);

    /**
     * 创建元数据表
     *
     * @param tableName 表名
     */
    void createMetadataTable(String tableName);

    /**
     * 更新元数据表
     *
     * @param entity 内容
     * @return 1：成功，0：失败
     */
    int updateMetadata(SlaveMetaDataEntity entity);

    /**
     * 插入元数据表
     *
     * @param entity 内容
     * @return 1：成功，0：失败
     */
    int insertMetadata(SlaveMetaDataEntity entity);

    /**
     * 从元数据表删除指定表的所有数据
     *
     * @param tableName 表名
     * @return 1：成功，0：失败
     */
    int deleteMetadata(String tableName);

    /**
     * 从元数据表查询指定表的所有数据
     *
     * @param isUsed     是否被使用
     * @param tableNames 所有表
     * @return 元数据信息
     */
    List<SlaveMetaDataEntity> getSlaveMetadataList(Integer isUsed, String... tableNames);

    // /**
    //  * 获取表的基本信息
    //  * @param tableName 查询的表名
    //  * @return
    //  */
    // List<TableInfo> getTableInfo(String... tableName);
    //
    // /**
    //  * 获取表的索引信息
    //  *
    //  * @param tableName  表名
    //  * @return
    //  */
    // List<TableIndexInfo> getTableIndexInfo(String tableName);
    //
    // /**
    //  * 获取表的列
    //  *
    //  * @param tableName  表名
    //  * @return
    //  */
    // List<TableColumnInfo> getTableColumnInfo(String tableName);
    //
    // /**
    //  * 获取表的外键
    //  *
    //  * @param tableName  表名
    //  * @return
    //  */
    // List<TableForeignKeyInfo> getTableForeignKeyInfo(String tableName);
    //
    // /**
    //  * 获取表的检查约束
    //  *
    //  * @param tableName  表名
    //  * @return
    //  */
    // List<TableCheckConstraintInfo> getTableCheckConstraintInfo(String tableName);

    // /**
    //  * 统计表数据
    //  *
    //  * @param tableName  表名
    //  * @return
    //  */
    // long selectCount(String tableName);
    //
    // /**
    //  * 查询数据
    //  *
    //  * @param tableName  表名
    //  * @param pageNum    页码
    //  * @param pageSize   每页大小
    //  * @return
    //  */
    // List<LinkedHashMap<String, Object>> selectList(String tableName,
    //                                                int pageNum,
    //                                                int pageSize);


    //////////////////////////////// SLAVE //////////////////////////////////


    // /**
    //  * 删除表
    //  *
    //  * @param tableInfo 表信息
    //  * @return
    //  */
    // void dropTable(TableInfo tableInfo);
    //
    // /**
    //  * 创建表
    //  *
    //  * @param tableInfo 表信息
    //  * @return
    //  */
    // void createTable(TableInfo tableInfo);
    //
    // /**
    //  * 查询表的数量
    //  *
    //  * @param tableName  表名
    //  * @return
    //  */
    // int countTable(String tableName);
    //
    // /**
    //  * 批量插入
    //  */
    // default void batchInsert(String tableName, List<LinkedHashMap<String, Object>> rows) {
    //
    //     StringBuilder columnNames = new StringBuilder();
    //     StringBuilder placeHolders = new StringBuilder();
    //     List<Object[]> rowValues = new ArrayList<>();
    //
    //     int i = 0;
    //     int j;
    //     for (LinkedHashMap<String, Object> row : rows) {
    //         Object[] values = new Object[row.size()];
    //         j = 0;
    //         for (Iterator<Map.Entry<String, Object>> it = row.entrySet().iterator(); it.hasNext(); j++) {
    //             Map.Entry<String, Object> en = it.next();
    //             if (i == 0) {
    //                 columnNames.append(en.getKey());
    //                 placeHolders.append("?");
    //                 if (j != values.length - 1) {
    //                     placeHolders.append(", ");
    //                     columnNames.append(", ");
    //                 }
    //             }
    //
    //             values[j] = en.getValue();
    //         }
    //         rowValues.add(values);
    //         i++;
    //     }
    //
    //     final String sql = String.format("insert into %s (%s) values (%s)", tableName, columnNames.toString(), placeHolders.toString());
    //
    //     if (logger.isInfoEnabled()) {
    //         logger.info("SQL: {}", sql);
    //     }
    //     try {
    //         this.getJdbcTemplate().batchUpdate(sql, rowValues);
    //
    //     } catch (Exception e) {
    //         e.printStackTrace();
    //         if (logger.isErrorEnabled()) {
    //             logger.error("批量插入失败: {}", e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
    //         }
    //         if (logger.isInfoEnabled()) {
    //             logger.info("已跳过。");
    //         }
    //     }
    //
    // }
}
