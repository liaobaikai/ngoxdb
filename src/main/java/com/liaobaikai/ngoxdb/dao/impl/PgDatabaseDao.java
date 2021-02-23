package com.liaobaikai.ngoxdb.dao.impl;

import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
import com.liaobaikai.ngoxdb.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.info.ConstraintInfo;
import com.liaobaikai.ngoxdb.info.DatabaseInfo;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

/**
 * Pg database数据访问
 * @author baikai.liao
 * @Time 2021-02-01 15:53:47
 */
public class PgDatabaseDao extends BasicDatabaseDao {

    @Getter
    enum Statement {

        // 查询数据库支持的字符集
        QUERY_SUPPORT_CHARSET("SELECT UNIQUE VALUE FROM V$NLS_VALID_VALUES WHERE PARAMETER = ?"),

        // 查询数据库字符集信息
        QUERY_DATABASE_CHARSET("SELECT " +
                "DATNAME TABLE_CAT, " +
                "NULL AS TABLE_SCHEM, " +
                "PG_ENCODING_TO_CHAR(ENCODING) AS CHARSET_NAME " +
                "FROM PG_DATABASE WHERE DATNAME = ?"),

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

        ;

        private final String statement;
        Statement(String statement) {
            this.statement = statement;
        }

    }

    private final DatabaseInfo databaseInfo;

    public PgDatabaseDao(JdbcTemplate2 jdbcTemplate) {
        super(jdbcTemplate);
        this.databaseInfo = this.initDatabaseInfo();
    }

    @Override
    protected DatabaseInfo initDatabaseInfo() {
        // https://www.postgresql.org/docs/13/datatype-character.html
        String databaseName = this.getJdbcTemplate().getDatabaseConfig().getDatabaseName();
        DatabaseInfo databaseInfo = this.getJdbcTemplate().queryForList2(
                Statement.QUERY_DATABASE_CHARSET.statement, DatabaseInfo.class, databaseName).get(0);

        // The storage requirement for a short string (up to 126 bytes) is 1 byte plus the actual string, which includes the space padding in the case of character.
        // Longer strings have 4 bytes of overhead instead of 1.
        databaseInfo.setMaxLen(4);

        return databaseInfo;
    }

    @Override
    public DatabaseInfo getDatabaseInfo() {
        return this.databaseInfo;
    }

    @Override
    public List<ConstraintInfo> getConstraintInfo(String... tableName) {
        return new ArrayList<>();
    }
}
