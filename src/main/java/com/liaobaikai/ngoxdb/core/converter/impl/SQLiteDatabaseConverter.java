package com.liaobaikai.ngoxdb.core.converter.impl;

import com.liaobaikai.ngoxdb.bean.NgoxDbMaster;
import com.liaobaikai.ngoxdb.core.comparator.DatabaseComparator;
import com.liaobaikai.ngoxdb.core.comparator.impl.SQLiteDatabaseComparator;
import com.liaobaikai.ngoxdb.core.converter.BasicDatabaseConverter;
import com.liaobaikai.ngoxdb.core.dao.DatabaseDao;
import com.liaobaikai.ngoxdb.core.dao.impl.SQLiteDatabaseDao;
import com.liaobaikai.ngoxdb.core.dialect.DatabaseDialect;
import com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

/**
 * SQLite数据库转换器
 *
 * @author baikai.liao
 * @Time 2021-02-25 22:32:37
 */
@Slf4j
@Service
public class SQLiteDatabaseConverter extends BasicDatabaseConverter {

    private final SQLiteDatabaseDao databaseDao;
    private final SQLiteDatabaseComparator databaseComparator;

    public SQLiteDatabaseConverter() {
        this.databaseDao = null;
        this.databaseComparator = null;
    }

    public SQLiteDatabaseConverter(NgoxDbMaster ngoxDbMaster) {
        super(ngoxDbMaster);
        this.databaseDao = new SQLiteDatabaseDao(ngoxDbMaster);
        this.databaseComparator = new SQLiteDatabaseComparator(this);
    }

    @Override
    public Logger getLogger() {
        return log;
    }

    @Override
    public DatabaseDao getDatabaseDao() {
        return this.databaseDao;
    }

    @Override
    public Class<? extends DatabaseDialect> getDatabaseDialectClass() {
        return null;
    }

    @Override
    public String getDatabaseVendor() {
        return DatabaseVendorEnum.SQLITE.getVendor();
    }

    @Override
    public DatabaseComparator getComparator() {
        return this.databaseComparator;
    }


}
