package com.liaobaikai.ngoxdb.dao.impl;

import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
import com.liaobaikai.ngoxdb.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.info.ConstraintInfo;
import com.liaobaikai.ngoxdb.info.DatabaseInfo;

import java.util.List;

/**
 * @author baikai.liao
 * @Time 2021-02-25 22:17:05
 */
public class SQLiteDatabaseDao extends BasicDatabaseDao {

    private final DatabaseInfo databaseInfo;

    public SQLiteDatabaseDao(JdbcTemplate2 jdbcTemplate) {
        super(jdbcTemplate);
        this.databaseInfo = initDatabaseInfo();
    }

    @Override
    protected DatabaseInfo initDatabaseInfo() {
        return null;
    }

    @Override
    public DatabaseInfo getDatabaseInfo() {
        return this.databaseInfo;
    }

    @Override
    public List<ConstraintInfo> getConstraintInfo(String... tableName) {
        return null;
    }
}
