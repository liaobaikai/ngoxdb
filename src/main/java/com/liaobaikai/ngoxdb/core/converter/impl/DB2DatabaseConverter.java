package com.liaobaikai.ngoxdb.core.converter.impl;

import com.liaobaikai.ngoxdb.bean.NgoxDbMaster;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.core.comparator.DatabaseComparator;
import com.liaobaikai.ngoxdb.core.comparator.impl.DB2DatabaseComparator;
import com.liaobaikai.ngoxdb.core.converter.BasicDatabaseConverter;
import com.liaobaikai.ngoxdb.core.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.core.dialect.DatabaseDialect;
import com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

/**
 * @author baikai.liao
 * @Time 2021-03-11 00:45:35
 */
@Slf4j
@Service
public class DB2DatabaseConverter extends BasicDatabaseConverter {

    private final DB2DatabaseComparator databaseComparator;

    public DB2DatabaseConverter() {
        this.databaseComparator = null;
    }

    public DB2DatabaseConverter(NgoxDbMaster ngoxDbMaster) {
        super(ngoxDbMaster);
        this.databaseComparator = new DB2DatabaseComparator(this);
    }

    @Override
    public Logger getLogger() {
        return log;
    }

    @Override
    public BasicDatabaseDao getDatabaseDao() {
        return null;
    }

    @Override
    public Class<? extends DatabaseDialect> getDatabaseDialectClass() {
        return null;
    }

    @Override
    public String getDatabaseVendor() {
        return DatabaseVendorEnum.DB2.getVendor();
    }

    @Override
    public DatabaseComparator getComparator() {
        return null;
    }

    @Override
    public void buildComment(TableInfo ti) {
        super.buildComment(ti);
    }
}
