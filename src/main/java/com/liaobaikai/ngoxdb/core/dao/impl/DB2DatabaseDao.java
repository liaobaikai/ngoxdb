package com.liaobaikai.ngoxdb.core.dao.impl;

import com.liaobaikai.ngoxdb.bean.NgoxDbMaster;
import com.liaobaikai.ngoxdb.bean.info.ConstraintInfo;
import com.liaobaikai.ngoxdb.bean.info.DatabaseInfo;
import com.liaobaikai.ngoxdb.core.dao.BasicDatabaseDao;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.util.List;

/**
 * @author baikai.liao
 * @Time 2021-03-11 00:49:51
 */
@Slf4j
public class DB2DatabaseDao extends BasicDatabaseDao {

    private final DatabaseInfo databaseInfo;

    public DB2DatabaseDao(NgoxDbMaster ngoxDbMaster) {
        super(ngoxDbMaster);
        this.databaseInfo = initDatabaseInfo();
    }

    @Override
    protected DatabaseInfo initDatabaseInfo() {
        DatabaseInfo databaseInfo = new DatabaseInfo();
        databaseInfo.setTableCat("PUBLIC");
        databaseInfo.setTableSchem("PUBLIC");
        databaseInfo.setMaxLen(2);
        databaseInfo.setCharsetName("GBK");
        return databaseInfo;
    }

    @Override
    public Logger getLogger() {
        return log;
    }

    @Override
    public String getSchema() {
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
