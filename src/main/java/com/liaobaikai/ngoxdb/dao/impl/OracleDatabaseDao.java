package com.liaobaikai.ngoxdb.dao.impl;

import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
import com.liaobaikai.ngoxdb.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.info.ConstraintInfo;
import com.liaobaikai.ngoxdb.info.DatabaseInfo;
import lombok.Getter;
import oracle.jdbc.OracleConnection;
import org.springframework.jdbc.core.ConnectionCallback;

import java.util.ArrayList;
import java.util.List;

/**
 * Oracle数据库访问
 *
 * @author baikai.liao
 * @Time 2021-01-31 23:33:15
 */
public class OracleDatabaseDao extends BasicDatabaseDao {

    @Getter
    enum Statement {

        // 查询数据库支持的字符集
        // QUERY_SUPPORT_CHARSET("SELECT UNIQUE VALUE FROM V$NLS_VALID_VALUES WHERE PARAMETER = ?"),

        // 查询数据库字符集信息
        QUERY_DATABASE_CHARSET("SELECT " +
                "NULL AS TABLE_CAT, " +
                "USER AS TABLE_SCHEM, " +
                "USERENV('LANGUAGE') CHARSET_NAME, " +
                "LENGTHB('\uD842\uDFB7') MAX_LEN " +
                "FROM DUAL"),

        // QUERY_ALL_TABLE("SELECT " +
        //         "NULL TABLE_CAT, " +
        //         "T.OWNER TABLE_SCHEM, " +
        //         "T.TABLE_NAME, " +
        //         "TC.TABLE_TYPE," +
        //         "TC.COMMENTS REMARKS " +
        //         "FROM ALL_TABLES T LEFT JOIN ALL_TAB_COMMENTS TC " +
        //         "ON T.OWNER = TC.OWNER AND T.TABLE_NAME = TC.TABLE_NAME " +
        //         "WHERE T.OWNER = ? "),
        //
        // QUERY_TABLE(QUERY_ALL_TABLE.getStatement() + "AND T.TABLE_NAME IN(?)"),

        QUERY_ALL_CHECK_CONSTRAINT("SELECT " +
                "NULL AS TABLE_CAT, " +
                "OWNER AS TABLE_SCHEM, " +
                "TABLE_NAME, " +
                "CONSTRAINT_NAME, " +
                "SEARCH_CONDITION AS CHECK_CONDITION, " +
                "CONSTRAINT_TYPE " +
                "FROM ALL_CONSTRAINTS WHERE OWNER = ? AND CONSTRAINT_TYPE = 'C'"),

        QUERY_CHECK_CONSTRAINT(QUERY_ALL_CHECK_CONSTRAINT.getStatement() + " AND TABLE_NAME IN (?)"),


        // 查询表的列的其他信息
        // QUERY_ALL_COLUMN_EXTEND("SELECT " +
        //         " COLUMN_NAME, " +
        //         " COLUMN_TYPE, " +
        //         " CHARACTER_SET_NAME AS CHARSET_NAME, " +
        //         " COLLATION_NAME, " +
        //         " COLUMN_DEFAULT AS COLUMN_DEF, " +
        //         " EXTRA " +
        //         " FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ?"),
        // QUERY_COLUMN_EXTEND(QUERY_ALL_COLUMN_EXTEND.statement + "AND TABLE_NAME in(?)"),

        ;

        private final String statement;
        Statement(String statement) {
            this.statement = statement;
        }

    }

    private final DatabaseInfo databaseInfo;
    private final String userName;

    public OracleDatabaseDao(JdbcTemplate2 jdbcTemplate) {
        super(jdbcTemplate);

        // oracle jdbc源码：
        // oracle.jdbc.OracleDatabaseMetaData.getTables()
        // oracle.jdbc.driver.OracleDatabaseMetaData.getColumns()
        userName = this.getJdbcTemplate().execute((ConnectionCallback<String>) con -> {
            if(con.isWrapperFor(OracleConnection.class)){
                OracleConnection oConnection = con.unwrap(OracleConnection.class);
                oConnection.setRemarksReporting(true);
                return oConnection.getUserName();
            }
            return con.getMetaData().getUserName();
        });

        this.databaseInfo = this.initDatabaseInfo();
    }

    @Override
    protected DatabaseInfo initDatabaseInfo() {
        // 数据库默认字符集
        // 数据库支持的字符集
        // https://docs.oracle.com/goldengate/1212/gg-winux/GWUAD/wu_charsets.htm#GWUAD733
        return this.getJdbcTemplate()
                .queryForList2(Statement.QUERY_DATABASE_CHARSET.statement, DatabaseInfo.class).get(0);
    }

    @Override
    public DatabaseInfo getDatabaseInfo() {
        return this.databaseInfo;
    }

    @Override
    public String getSchemaPattern() {
        return this.userName;
    }

    // @Override
    // public List<TableInfo> getTables(String... tableName) {
    //     // oracle.jdbc.OracleDatabaseMetaData.getTables()
    //
    //     final StringBuilder sqlBuilder = new StringBuilder(
    //             (tableName.length == 0 ? OracleStatement.QUERY_ALL_TABLE : OracleStatement.QUERY_TABLE).getStatement());
    //     Object[] params = this.getParams(sqlBuilder, tableName, null, this.getSchemaPattern());
    //
    //     return this.getJdbcTemplate().queryForList2(sqlBuilder.toString(), TableInfo.class, params);
    // }
    //
    // @Override
    // public List<ColumnInfo> getColumns(String... tableName) {
    //     // oracle.jdbc.driver.OracleDatabaseMetaData.getColumns()
    //     return super.getColumns(tableName);
    // }

    @Override
    public List<ConstraintInfo> getConstraintInfo(String... tableName) {

        StringBuilder sqlBuilder = new StringBuilder(
                (tableName.length > 0 ? Statement.QUERY_CHECK_CONSTRAINT : Statement.QUERY_ALL_CHECK_CONSTRAINT).getStatement());

        Object[] params = this.getParams(sqlBuilder, tableName, null, this.getSchemaPattern());

        final List<ConstraintInfo> resultList = this.getJdbcTemplate().queryForList2(sqlBuilder.toString(), ConstraintInfo.class, params);

        // 1.名称问题
        // 2.过滤非空约束，如"USER_NAME" IS NOT NULL
        List<ConstraintInfo> finalResultList = new ArrayList<>();
        resultList.forEach(ci -> {
            if(!ci.getCheckCondition().endsWith("IS NOT NULL")){
                ci.setCheckCondition(String.format("(%s)", ci.getCheckCondition()));
                finalResultList.add(ci);
            }
        });

        return finalResultList;
    }

    @Override
    public long getTableRowCount(String schema, String tableName) {
        return super.getTableRowCount(schema, tableName.toUpperCase());
    }
}
