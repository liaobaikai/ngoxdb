package com.liaobaikai.ngoxdb.core.converter.impl;

import com.liaobaikai.ngoxdb.bean.NgoxDbMaster;
import com.liaobaikai.ngoxdb.core.comparator.DatabaseComparator;
import com.liaobaikai.ngoxdb.core.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.core.dao.impl.MariaDbDatabaseDao;
import com.liaobaikai.ngoxdb.core.dialect.DatabaseDialect;
import com.liaobaikai.ngoxdb.core.dialect.impl.MariaDbDatabaseDialect;
import com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

/**
 * MariaDB 转换器
 *
 * @author baikai.liao
 * @Time 2021-01-28 15:42:06
 */
@Slf4j
@Service
public class MariaDbDatabaseConverter extends MySQLDatabaseConverter {

    private final MariaDbDatabaseDao databaseDao;

    public MariaDbDatabaseConverter() {
        this.databaseDao = null;
    }

    public MariaDbDatabaseConverter(NgoxDbMaster ngoxDbMaster) {
        super(ngoxDbMaster);
        this.databaseDao = new MariaDbDatabaseDao(ngoxDbMaster);
    }

    @Override
    public String getDatabaseVendor() {
        return DatabaseVendorEnum.MARIADB.getVendor();
    }

    @Override
    public DatabaseComparator getComparator() {
        return null;
    }

    @Override
    public Logger getLogger() {
        return log;
    }

    @Override
    public BasicDatabaseDao getDatabaseDao() {
        return this.databaseDao;
    }

    @Override
    public Class<? extends DatabaseDialect> getDatabaseDialectClass() {
        return MariaDbDatabaseDialect.class;
    }
}
