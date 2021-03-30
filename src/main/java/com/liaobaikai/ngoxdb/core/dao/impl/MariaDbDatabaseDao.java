package com.liaobaikai.ngoxdb.core.dao.impl;

import com.liaobaikai.ngoxdb.bean.NgoxDbMaster;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

/**
 * @author baikai.liao
 * @Time 2021-02-04 15:10:36
 */
@Slf4j
public class MariaDbDatabaseDao extends MySQLDatabaseDao {

    public MariaDbDatabaseDao(NgoxDbMaster ngoxDbMaster) {
        super(ngoxDbMaster);
    }

    @Override
    public Logger getLogger() {
        return log;
    }

}
