package com.liaobaikai.ngoxdb.core.dao.impl;

import com.liaobaikai.ngoxdb.bean.info.ConstraintInfo;
import com.liaobaikai.ngoxdb.bean.info.DatabaseInfo;
import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
import com.liaobaikai.ngoxdb.core.dao.BasicDatabaseDao;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.util.List;

/**
 * @author baikai.liao
 * @Time 2021-02-25 22:17:05
 */
@Slf4j
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
    public Logger getLogger() {
        return log;
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
