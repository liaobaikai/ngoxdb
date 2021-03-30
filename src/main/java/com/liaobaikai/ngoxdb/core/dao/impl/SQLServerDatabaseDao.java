package com.liaobaikai.ngoxdb.core.dao.impl;

import com.liaobaikai.ngoxdb.bean.NgoxDbMaster;
import com.liaobaikai.ngoxdb.bean.info.ColumnInfo;
import com.liaobaikai.ngoxdb.bean.info.ConstraintInfo;
import com.liaobaikai.ngoxdb.bean.info.DatabaseInfo;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.core.dao.BasicDatabaseDao;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.util.List;

/**
 * SQLServer数据库访问
 *
 * @author baikai.liao
 * @Time 2021-01-27 16:23:27
 */
@Slf4j
public class SQLServerDatabaseDao extends BasicDatabaseDao {

    @Getter
    enum Statement {

        // 查询数据库字符集信息
        // SQLServer不支持字符 "𠮷", 保存后查询变成 ??
        //
        // QUERY_DATABASE_CHARSET("SELECT " +
        //         "DB_NAME() AS TABLE_CAT, " +
        //         "SCHEMA_NAME() AS TABLE_SCHEM, " +
        //         "SERVERPROPERTY('SqlCharSetName') CHARSET_NAME, " +
        //         "DATALENGTH('國') MAX_LEN"),

        // 检查约束
        QUERY_ALL_CHECK_CONSTRAINT("SELECT " +
                "DB_NAME() AS TABLE_CAT, " +
                "SCHEMA_NAME(CC.SCHEMA_ID) AS TABLE_SCHEM, " +
                "OBJECT_NAME(CC.PARENT_OBJECT_ID) TABLE_NAME, " +
                "CC.NAME CONSTRAINT_NAME, " +
                "CC.DEFINITION AS CHECK_CONDITION, " +
                "'C' CONSTRAINT_TYPE " +
                "FROM SYS.CHECK_CONSTRAINTS CC WHERE 1 = 1"),

        QUERY_CHECK_CONSTRAINT(QUERY_ALL_CHECK_CONSTRAINT.statement + "AND PARENT_OBJECT_ID IN (?)"),

        QUERY_ALL_COLUMN_EXTEND("SELECT " +
                "DB_NAME() AS TABLE_CAT," +
                "SCHEMA_NAME(SO.SCHEMA_ID) TABLE_SCHEM, " +
                "SO.NAME TABLE_NAME, " +
                "SC.NAME AS COLUMN_NAME, " +
                "SC.COLLATION AS COLLATION_NAME, " +
                "SEP.VALUE REMARKS " +
                "FROM SYS.OBJECTS SO, SYSCOLUMNS SC, SYS.EXTENDED_PROPERTIES SEP " +
                "WHERE SC.ID = SO.OBJECT_ID AND SC.ID = SEP.MAJOR_ID AND SC.COLID = SEP.MINOR_ID"),

        QUERY_COLUMN_EXTEND(QUERY_ALL_COLUMN_EXTEND.statement + " AND SO.NAME IN (?)"),

        // 查询表的注释
        QUERY_ALL_TABLE_EXTEND("SELECT " +
                "DB_NAME() AS TABLE_CAT, " +
                "SCHEMA_NAME(t.SCHEMA_ID) TABLE_SCHEM, " +
                "T.NAME TABLE_NAME, " +
                "EP.VALUE REMARKS,\n" +
                "SERVERPROPERTY('Collation') TABLE_COLLATION, " +
                "SERVERPROPERTY('SqlCharSetName') TABLE_CHARSET " +
                "FROM SYS.TABLES T " +
                "LEFT JOIN SYS.EXTENDED_PROPERTIES EP " +
                "ON T.OBJECT_ID = EP.MAJOR_ID AND EP.MINOR_ID = 0"),

        QUERY_TABLE_EXTEND(QUERY_ALL_TABLE_EXTEND.statement + " WHERE T.NAME IN(?)"),

        ;

        private final String statement;

        Statement(String statement) {
            this.statement = statement;
        }

    }

    private final DatabaseInfo databaseInfo;

    public SQLServerDatabaseDao(NgoxDbMaster ngoxDbMaster) {
        super(ngoxDbMaster);
        this.databaseInfo = initDatabaseInfo();
    }

    @Override
    protected DatabaseInfo initDatabaseInfo() {
        // 数据库默认字符集
        // return this.getJdbcTemplate()
        //         .queryForList(Statement.QUERY_DATABASE_CHARSET.statement, DatabaseInfo.class).get(0);
        return null;
    }

    @Override
    public Logger getLogger() {
        return log;
    }

    @Override
    public String getSchema() {
        return "dbo";
    }

    @Override
    public DatabaseInfo getDatabaseInfo() {
        return this.databaseInfo;
    }

    @Override
    public List<TableInfo> getTables(String... tableName) {
        List<TableInfo> tables = super.getTables(tableName);

        // 查询表注释、以及默认排序规则信息
        StringBuilder sqlBuilder = new StringBuilder(
                (tableName.length == 0 ? Statement.QUERY_ALL_TABLE_EXTEND : Statement.QUERY_TABLE_EXTEND).getStatement());

        Object[] params = this.getParams(sqlBuilder, tableName, null, null);

        List<TableInfo> tableList = this.getJdbcTemplate().queryForList(sqlBuilder.toString(), TableInfo.class, params);

        // 表注释
        tables.forEach(table -> tableList.forEach(table2 -> {
            if (table.getTableName().equals(table2.getTableName())) {
                table.setTableCollation(table2.getTableCollation());
                table.setRemarks(table2.getRemarks());
            }
        }));

        return tables;
    }

    @Override
    public List<ColumnInfo> getColumns(String... tableName) {
        List<ColumnInfo> columns = super.getColumns(tableName);

        // https://github.com/microsoft/mssql-jdbc/issues/646

        StringBuilder sqlBuilder = new StringBuilder(
                (tableName.length == 0 ? Statement.QUERY_ALL_COLUMN_EXTEND : Statement.QUERY_COLUMN_EXTEND).getStatement());

        Object[] params = this.getParams(sqlBuilder, tableName, null, null);

        // 合并数据
        List<ColumnInfo> columnInfoList = this.getJdbcTemplate().queryForList(sqlBuilder.toString(), ColumnInfo.class, params);
        columnInfoList.forEach(columnInfo -> {


            // 合并列排序规则和列注释
            columns.forEach(column -> {
                boolean isValidSchema = columnInfo.getTableSchem() == null ||
                        (columnInfo.getTableSchem() != null && columnInfo.getTableSchem().equals(column.getTableSchem()));
                if (isValidSchema
                        && columnInfo.getTableName().equals(column.getTableName())
                        && columnInfo.getColumnName().equals(column.getColumnName())) {
                    // 表名和列名相同
                    column.setCollationName(columnInfo.getCollationName());
                    column.setRemarks(columnInfo.getRemarks());
                }
            });

        });

        return columns;
    }

    @Override
    public List<ConstraintInfo> getConstraintInfo(String... tableName) {
        StringBuilder sqlBuilder = new StringBuilder(
                (tableName.length == 0 ? Statement.QUERY_ALL_CHECK_CONSTRAINT : Statement.QUERY_CHECK_CONSTRAINT).getStatement());

        Object[] params = this.getParams(sqlBuilder, tableName, "OBJECT_ID(?)", null);

        List<ConstraintInfo> resultList = this.getJdbcTemplate().queryForList(sqlBuilder.toString(), ConstraintInfo.class, params);

        // 1.名称问题
        // 2.过滤非空约束
        resultList.forEach(ci -> {
            if (ci.getConstraintName().endsWith("IS NOT NULL")) {

            }
        });

        return resultList;
    }

    // @Override
    // public void dropForeignKey(String tableName) {
    //
    //     // 删除表前查询引用关系，如果存在其他表有引用的话，需要删除表的引用。
    //     List<Map<String, Object>> queryResponse = this.getJdbcTemplate()
    //             .queryForList("select fk.name as NAME," +
    //                     "fk.object_id," +
    //                     "OBJECT_NAME(fk.parent_object_id) as REFERENCE_TABLE_NAME " +
    //                     "from sys.foreign_keys as fk join sys.objects as o " +
    //                     "on fk.referenced_object_id=o.object_id where o.name = ?", tableName);
    //
    //     // 删除表的约束
    //     for (Map<String, Object> row : queryResponse) {
    //         String refTable = row.get("REFERENCE_TABLE_NAME").toString();
    //         String fkName = row.get("NAME").toString();
    //         getLogger().info("Table {} exists foreign key, ALTER TABLE [{}] DROP CONSTRAINT {}", tableName, refTable, fkName);
    //
    //         this.execute("ALTER TABLE [" + refTable + "] DROP CONSTRAINT [" + fkName + "]");
    //     }
    // }
}
