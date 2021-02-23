package com.liaobaikai.ngoxdb.dao.impl;

import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;

/**
 * @author baikai.liao
 * @Time 2021-02-04 15:10:36
 */
public class MariaDBDatabaseDao extends MySQLDatabaseDao {

    public MariaDBDatabaseDao(JdbcTemplate2 jdbcTemplate) {
        super(jdbcTemplate);
    }
}
