package com.liaobaikai.ngoxdb.core.dao.impl;

import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

/**
 * @author baikai.liao
 * @Time 2021-02-04 15:10:36
 */
@Slf4j
public class MariaDBDatabaseDao extends MySQLDatabaseDao {

    public MariaDBDatabaseDao(JdbcTemplate2 jdbcTemplate) {
        super(jdbcTemplate);
    }

    @Override
    public Logger getLogger() {
        return log;
    }
}
