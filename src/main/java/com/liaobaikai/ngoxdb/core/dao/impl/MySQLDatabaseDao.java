package com.liaobaikai.ngoxdb.core.dao.impl;

import com.liaobaikai.ngoxdb.bean.NgoxDbMaster;
import com.liaobaikai.ngoxdb.bean.info.*;
import com.liaobaikai.ngoxdb.bean.rs.ImportedKey;
import com.liaobaikai.ngoxdb.core.constant.JdbcDataType;
import com.liaobaikai.ngoxdb.core.dao.BasicDatabaseDao;
import com.mysql.cj.MysqlType;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * MySQL数据访问
 *
 * @author baikai.liao
 * @Time 2021-01-27 16:22:52
 */
@Slf4j
public class MySQLDatabaseDao extends BasicDatabaseDao {

    @Getter
    enum Statement {

        // 查询数据库字符集信息
        QUERY_DATABASE_CHARSET("SELECT " +
                "S.SCHEMA_NAME AS TABLE_CAT, " +
                "NULL AS TABLE_SCHEM, " +
                "S.DEFAULT_CHARACTER_SET_NAME AS CHARSET_NAME, " +
                "CS.MAXLEN " +
                "FROM INFORMATION_SCHEMA.SCHEMATA S LEFT JOIN INFORMATION_SCHEMA.CHARACTER_SETS CS " +
                "ON S.DEFAULT_CHARACTER_SET_NAME = CS.CHARACTER_SET_NAME " +
                "WHERE S.SCHEMA_NAME = ?"),

        // 检查约束
        QUERY_ALL_CHECK_CONSTRAINT("SELECT" +
                " CONSTRAINT_SCHEMA AS TABLE_CAT, " +
                " NULL AS TABLE_SCHEM, " +
                " TABLE_NAME, " +
                " CONSTRAINT_NAME, " +
                " CHECK_CLAUSE AS CHECK_CONDITION, " +
                " 'C' CONSTRAINT_TYPE" +
                " FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = ?"),

        QUERY_CHECK_CONSTRAINT(QUERY_ALL_CHECK_CONSTRAINT.statement + "  AND TABLE_NAME IN (?)"),

        // 查询创建视图的语句
        CREATE_VIEW_DEFINITION("SELECT " +
                " REPLACE(REPLACE(VIEW_DEFINITION, CONCAT('`', TABLE_SCHEMA, '`.'), ''), '`', '') AS VIEW_DEFINITION " +
                " FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"),
        // 查询表的列的其他信息
        QUERY_ALL_COLUMN_EXTEND("SELECT " +
                " COLUMN_NAME, " +
                " COLUMN_TYPE, " +
                " CHARACTER_SET_NAME AS CHARSET_NAME, " +
                " COLLATION_NAME, " +
                " COLUMN_DEFAULT AS COLUMN_DEF, " +
                " EXTRA " +
                " FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ?"),
        QUERY_COLUMN_EXTEND(QUERY_ALL_COLUMN_EXTEND.statement + "AND TABLE_NAME in(?)"),

        // 查询索引的其他信息
        QUERY_INDEX_EXTEND("SELECT " +
                " TABLE_NAME, " +
                " COLUMN_NAME, " +
                " INDEX_NAME, " +
                " CASE WHEN INDEX_TYPE = 'BTREE' OR INDEX_TYPE = 'RTREE' THEN 3 WHEN INDEX_TYPE = 'HASH' THEN 2 WHEN INDEX_TYPE = 'FULLTEXT' THEN 10 WHEN INDEX_TYPE = 'SPATIAL' THEN 11 END TYPE " +
                " FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"),
        // 查询表的信息
        QUERY_TABLE_EXTEND("SELECT " +
                " TABLE_NAME, " +
                " AUTO_INCREMENT, " +
                " TABLE_COLLATION, " +
                " CREATE_OPTIONS " +
                " FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = ?"),
        // 查询表的分区
        // QUERY_TABLE_PARTITIONS("SELECT TABLE_NAME," +
        //         "PARTITION_ORDINAL_POSITION, PARTITION_METHOD,PARTITION_EXPRESSION,PARTITION_NAME,PARTITION_DESCRIPTION," +
        //         "SUBPARTITION_ORDINAL_POSITION, SUBPARTITION_METHOD,SUBPARTITION_EXPRESSION,SUBPARTITION_NAME,SUBPARTITION_EXPRESSION," +
        //         "PARTITION_COMMENT " +
        //         "FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? " +
        //         "ORDER BY PARTITION_ORDINAL_POSITION ASC, SUBPARTITION_ORDINAL_POSITION ASC"),

        // 将表修改为分区表
        // TABLE_NAME, PARTITION_METHOD, PARTITION_EXPRESSION, PARTITION_NAME, PARTITION_DESCRIPTION
        // ALTER_TABLE_RANGE_PARTITION("ALTER TABLE %s PARTITION BY %s(%s) (PARTITION %s VALUES LESS THAN (%s))"),
        // ALTER_TABLE_HASH_PARTITION("ALTER TABLE %s PARTITION BY %s(%s) (PARTITION %s)"),

        ;

        private final String statement;

        Statement(String statement) {
            this.statement = statement;
        }
    }


    private final DatabaseInfo databaseInfo;

    public MySQLDatabaseDao(NgoxDbMaster ngoxDbMaster) {
        super(ngoxDbMaster);
        this.databaseInfo = this.initDatabaseInfo();
    }

    @Override
    protected DatabaseInfo initDatabaseInfo() {
        return this.getJdbcTemplate()
                .queryForList(Statement.QUERY_DATABASE_CHARSET.statement, DatabaseInfo.class, this.getSchema()).get(0);
    }

    @Override
    public Logger getLogger() {
        return log;
    }

    @Override
    public String getSchema() {
        return this.getNgoxDbMaster().getDatabaseConfig().getDatabaseName();
    }

    @Override
    public DatabaseInfo getDatabaseInfo() {
        return this.databaseInfo;
    }

    @Override
    public String getCreateViewDefinition(String viewName) {

        // List<LinkedHashMap<String, Object>> queryResponse = this.getJdbcTemplate().query(
        //         Statement.CREATE_VIEW_DEFINITION.getStatement(), this.getCatalog(), viewName);
        // if(queryResponse.size() == 0){
        //     return "";
        // }
        // return String.valueOf(queryResponse.get(0).get("VIEW_DEFINITION"));
        return null;
    }

    @Override
    public List<TableInfo> getTables(String... tableName) {
        List<TableInfo> tableInfoList = super.getTables(tableName);
        if (tableInfoList.size() == 0) {
            return tableInfoList;
        }

        // 查询 AUTO_INCREMENT, TABLE_COLLATION, CREATE_OPTIONS
        List<TableInfo> queryResponse =
                this.getJdbcTemplate().queryForList(Statement.QUERY_TABLE_EXTEND.getStatement(),
                        TableInfo.class, this.getSchema(), "BASE TABLE");

        tableInfoList.forEach(tableInfo -> {
            // 扩展信息
            for (TableInfo row : queryResponse) {
                if (tableInfo.getTableName().equals(row.getTableName())) {
                    tableInfo.setAutoIncrement(row.getAutoIncrement());
                    tableInfo.setTableCollation(row.getTableCollation());
                    tableInfo.setCreateOptions(row.getCreateOptions());
                    // if(createOptions.contains("partitioned")){
                    //     // 分区表
                    //     this.getJdbcTemplate().query(Statement.QUERY_TABLE_PARTITIONS.statement, catalog, tName);
                    // }
                    break;
                }
            }

        });

        return tableInfoList;
    }

    @Override
    public List<ColumnInfo> getColumns(String... tableName) {

        List<ColumnInfo> columnInfoList = super.getColumns(tableName);

        StringBuilder sqlBuilder = new StringBuilder(
                (tableName.length > 0 ? Statement.QUERY_COLUMN_EXTEND : Statement.QUERY_ALL_COLUMN_EXTEND).getStatement());

        Object[] params = this.getParams(sqlBuilder, tableName, null, this.getSchema());

        // 查询COLUMN_TYPE
        List<ColumnInfo> queryResponse = this.getJdbcTemplate().queryForList(
                sqlBuilder.toString(), ColumnInfo.class, params);

        columnInfoList.forEach(columnInfo -> queryResponse.forEach(row -> {
            if (columnInfo.getColumnName().equals(row.getColumnName())) {
                columnInfo.setColumnType(row.getColumnType());
                // 自动增长的信息: columnInfo.isAutoincrement
                if (!this.getDatabaseDialect().getIdentityColumnString().equalsIgnoreCase(row.getExtra())) {
                    columnInfo.setExtra(row.getExtra());
                }
                columnInfo.setCharsetName(row.getCharsetName());
                columnInfo.setCollationName(row.getCollationName());
                columnInfo.setColumnDef(row.getColumnDef());

                MysqlType mysqlType = MysqlType.getByName(row.getColumnType());
                switch (mysqlType) {
                    case TINYINT:
                        columnInfo.setSourceDataType(columnInfo.getDataType());
                        columnInfo.setDataType(JdbcDataType.TINYINT);
                        break;
                    case BIT:
                        columnInfo.setSourceDataType(columnInfo.getDataType());
                        columnInfo.setDataType(JdbcDataType.BIT);
                        break;
                    case ENUM:
                        columnInfo.setSourceDataType(columnInfo.getDataType());
                        columnInfo.setDataType(JdbcDataType.ENUM);
                        break;
                    case SET:
                        columnInfo.setSourceDataType(columnInfo.getDataType());
                        columnInfo.setDataType(JdbcDataType.SET);
                        break;
                    case JSON:
                        columnInfo.setSourceDataType(columnInfo.getDataType());
                        columnInfo.setDataType(JdbcDataType.JSON);
                        break;
                    case YEAR:
                        columnInfo.setSourceDataType(columnInfo.getDataType());
                        columnInfo.setDataType(JdbcDataType.YEAR);
                        break;
                    case DATETIME:
                        columnInfo.setSourceDataType(columnInfo.getDataType());
                        columnInfo.setDataType(JdbcDataType.DATETIME);
                        break;
                }
                if (mysqlType.isAllowed(MysqlType.FIELD_FLAG_UNSIGNED)) {
                    // 是否为无符号
                    columnInfo.setUnsigned(true);
                }
            }
        }));

        return columnInfoList;
    }


    @Override
    public List<IndexInfo2> getIndexInfo(String tableName) {

        List<IndexInfo2> indexInfoList = super.getIndexInfo(tableName);

        List<IndexInfo2> queryResponse = this.getJdbcTemplate().queryForList(
                Statement.QUERY_INDEX_EXTEND.getStatement(), IndexInfo2.class, this.getSchema(), tableName);

        indexInfoList.forEach(indexInfo -> queryResponse.forEach(row -> {
            if (indexInfo.getColumnName().equals(row.getColumnName())
                    && indexInfo.getIndexName().equals(row.getIndexName())) {
                indexInfo.setType(row.getType());
            }
        }));

        return indexInfoList;
    }

    @Override
    public void removePrimaryIndex(List<IndexInfo2> indexInfoList) {
        if (indexInfoList == null) {
            return;
        }

        // ref: https://dev.mysql.com/doc/refman/8.0/en/information-schema-statistics-table.html
        // The name of the index. If the index is the primary key, the name is always PRIMARY.
        indexInfoList.removeIf(indexInfo -> "PRIMARY".equals(indexInfo.getIndexName()));
    }

    @Override
    public List<ConstraintInfo> getConstraintInfo(String... tableName) {
        StringBuilder sqlBuilder = new StringBuilder(
                (tableName.length > 0 ? Statement.QUERY_CHECK_CONSTRAINT : Statement.QUERY_ALL_CHECK_CONSTRAINT).getStatement());

        Object[] params = this.getParams(sqlBuilder, tableName, null, this.getSchema());

        final List<ConstraintInfo> resultList = this.getJdbcTemplate().queryForList(sqlBuilder.toString(), ConstraintInfo.class, params);

        resultList.forEach(constraintInfo -> constraintInfo.setCheckCondition(String.format("(%s)", constraintInfo.getCheckCondition())));

        return resultList;
    }

    @Override
    public List<ImportedKey> getImportedKeys(String tableName) {
        // 存在外键的表，当多次删除表的时候，会查询到2条一样的数据。

        List<ImportedKey> importedKeys = super.getImportedKeys(tableName);
        List<ImportedKey> finalList = new ArrayList<>();

        // 去重
        boolean flag;
        for (ImportedKey importedKey : importedKeys) {
            flag = false;
            for (ImportedKey importedKey2 : finalList) {
                if (importedKey2.getFkName().equals(importedKey.getFkName())) {
                    flag = true;
                    break;
                }
            }
            if (!flag) {
                finalList.add(importedKey);
            }
        }

        return finalList;
    }

}
