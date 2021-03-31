package com.liaobaikai.ngoxdb.core.dao.impl;

import com.liaobaikai.ngoxdb.bean.NgoxDbMaster;
import com.liaobaikai.ngoxdb.bean.info.ConstraintInfo;
import com.liaobaikai.ngoxdb.bean.info.DatabaseInfo;
import com.liaobaikai.ngoxdb.core.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.core.dialect.impl.DmDatabaseDialect;
import dm.jdbc.driver.DMException;
import dm.jdbc.driver.DmdbConnection;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * 达梦数据库数据访问
 *
 * @author baikai.liao
 * @Time 2021-03-11 16:12:42
 */
@Slf4j
public class DmDatabaseDao extends BasicDatabaseDao {

    @Getter
    enum Statement {

        // 查询数据库支持的字符集
        // QUERY_SUPPORT_CHARSET("SELECT UNIQUE VALUE FROM V$NLS_VALID_VALUES WHERE PARAMETER = ?"),

        // 查询数据库字符集信息
        // QUERY_DATABASE_CHARSET("SELECT " +
        //         "NULL AS TABLE_CAT, " +
        //         "USER AS TABLE_SCHEM, " +
        //         "USERENV('LANGUAGE') CHARSET_NAME, " +
        //         "LENGTHB('\uD842\uDFB7') MAX_LEN " +
        //         "FROM DUAL"),

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
                "'(' || SEARCH_CONDITION || ')' AS CHECK_CONDITION, " +
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

    private DatabaseInfo databaseInfo;
    private String schema;

    public DmDatabaseDao(NgoxDbMaster ngoxDbMaster) {
        super(ngoxDbMaster);

        final int[] lengthInChar = new int[1];
        this.getJdbcTemplate().execute(new ConnectionCallback<Void>() {
            @Nullable
            @Override
            public Void doInConnection(@NonNull Connection con) throws SQLException, DataAccessException {

                if (con.isWrapperFor(DmdbConnection.class)) {
                    DmdbConnection dmdbConnection = con.unwrap(DmdbConnection.class);
                    schema = dmdbConnection.getSchema();

                    // 获取是否需要
                    con.createStatement().execute(String.format("create table %s (memo varchar(1))", "ngoxdb_dm_test"));
                    try {
                        con.createStatement().execute(String.format("insert into ngoxdb_dm_test values ('%s')", "\uD842\uDFB7"));
                        lengthInChar[0] = 1;
                    } catch (DMException e) {
                        if (e.getErrorCode() == -6169) {
                            // [-6169]:Column [memo] out of length.
                            // [-6169]:列[memo]长度超出定义
                            lengthInChar[0] = 0;
                        }
                    } finally {
                        con.createStatement().execute(String.format("drop table %s", "ngoxdb_dm_test"));
                    }

                }
                return null;
            }
        });

        if (schema == null || schema.length() == 0) {
            schema = this.getNgoxDbMaster().getDatabaseConfig().getUsername().toLowerCase();
        }

        // 设置
        if (ngoxDbMaster.getDatabaseDialect() instanceof DmDatabaseDialect) {
            ((DmDatabaseDialect) ngoxDbMaster.getDatabaseDialect()).setLengthInChar(lengthInChar[0]);
        }

    }

    @Override
    protected DatabaseInfo initDatabaseInfo() {
        return null;
    }

    @Override
    public Logger getLogger() {
        return log;
    }

    @Override
    public String getSchema() {
        return this.getNgoxDbMaster().getDatabaseConfig().getUsername().toUpperCase();
    }

    @Override
    public DatabaseInfo getDatabaseInfo() {
        return this.databaseInfo;
    }

    @Override
    public List<ConstraintInfo> getConstraintInfo(String... tableName) {

        StringBuilder sqlBuilder = new StringBuilder(
                (tableName.length > 0 ? Statement.QUERY_CHECK_CONSTRAINT : Statement.QUERY_ALL_CHECK_CONSTRAINT).getStatement());

        Object[] params = this.getParams(sqlBuilder, tableName, null, this.getSchema());

        return this.getJdbcTemplate().queryForList(sqlBuilder.toString(), ConstraintInfo.class, params);
    }


    // @Override
    // public void dropTable(String tableName) {
    //     this.execute(String.format("drop table %s cascade", getDatabaseDialect().toLookupName(tableName)));
    // }
}
