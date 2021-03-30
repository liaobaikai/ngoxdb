package com.liaobaikai.ngoxdb.core.converter.impl;

import com.liaobaikai.ngoxdb.bean.NgoxDbMaster;
import com.liaobaikai.ngoxdb.bean.info.ColumnInfo;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.bean.rs.ImportedKey;
import com.liaobaikai.ngoxdb.core.comparator.DatabaseComparator;
import com.liaobaikai.ngoxdb.core.comparator.impl.AccessDatabaseComparator;
import com.liaobaikai.ngoxdb.core.converter.BasicDatabaseConverter;
import com.liaobaikai.ngoxdb.core.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.core.dao.impl.AccessDatabaseDao;
import com.liaobaikai.ngoxdb.core.dialect.DatabaseDialect;
import com.liaobaikai.ngoxdb.core.dialect.impl.AccessDatabaseDialect;
import com.liaobaikai.ngoxdb.core.entity.NgoxDbRelayLog;
import com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Microsoft Access 转换器
 *
 * @author baikai.liao
 * @Time 2021-01-29 23:42:45
 */
@Slf4j
@Service
public class AccessDatabaseConverter extends BasicDatabaseConverter {

    private final AccessDatabaseDao databaseDao;
    private final AccessDatabaseComparator databaseComparator;
    private ReentrantLock reentrantLock;

    // 是否为空表
    private long tableRowCount;

    public AccessDatabaseConverter() {
        this.databaseDao = null;
        this.databaseComparator = null;
    }

    public AccessDatabaseConverter(NgoxDbMaster ngoxDbMaster) {
        super(ngoxDbMaster);
        // drop table command is invalid!!!!
        this.getLogger().warn("Drop table command is not compatible!!! Parameter {replace-table} always false!");
        this.getDatabaseConfig().setReplaceTable(false);
        this.databaseDao = new AccessDatabaseDao(ngoxDbMaster);
        this.databaseComparator = new AccessDatabaseComparator(this);
        reentrantLock = new ReentrantLock();
    }

    @Override
    public Logger getLogger() {
        return log;
    }

    @Override
    public void beforeExportRows(TableInfo ti) {

        this.tableRowCount = this.getDatabaseDao().getTableRowCount(ti.getTableName());
        if (tableRowCount > 0) {
            this.getLogger().info("[{}] skip table {}.", this.getDatabaseConfig().getName(), this.toLookupName(ti.getTableName()));
        }

        // https://stackoverflow.com/questions/44420840/change-autonumber-values-when-inserting-rows-with-ucanaccess/44424112#44424112
        // 3.0.0 Release
        // It may be useful in import/export of data from and to different tables with the same structure, avoiding to break some FK constraint.
        super.beforeExportRows(ti);
    }

    @Override
    public void importRows(List<Object[]> batchArgs, TableInfo ti, int offset, int limit) {

        if (tableRowCount > 0) {
            this.getLogger().info("[{}] table {}, skip {} rows.",
                    this.getDatabaseConfig().getName(), this.toLookupName(ti.getTableName()), batchArgs.size());
            return;
        }

        try {
            reentrantLock.lock();
            // https://stackoverflow.com/questions/44420840/change-autonumber-values-when-inserting-rows-with-ucanaccess/44424112#44424112
            // 3.0.0 Release
            // It may be useful in import/export of data from and to different tables with the same structure, avoiding to break some FK constraint.
            super.importRows(batchArgs, ti, offset, limit);
        } finally {
            reentrantLock.unlock();
        }

    }

    @Override
    protected void enableIdentity(Connection con, String tableName) {
        // 每次禁用后无需启用
        // 否则会抛出错误：
        // UCAExc:::5.0.1 integrity constraint violation: unique constraint or index violation; SYS_PK_10393 table: XXXXXX
    }

    @Override
    public void applyLog() {
        // 多线程会存在问题
        // BUG1. index has no columns
        // BUG2. duplicate index name: idx_...
        for (NgoxDbRelayLog ngoxDbRelayLog : this.removeDuplicate(this.getDatabaseDao().getLogRows(NgoxDbRelayLog.NOT_USED))) {
            apply(ngoxDbRelayLog);
        }
    }

    // @Override
    // public void exportRows(TableInfo ti, OnExportListener onExportListener) {
    //     // 禁用自动增长，不让返回的数据会从1按顺序返回，导致数据错误。
    //     // this.tableAutoIncrement(ti, false);
    //     // 迁移数据。
    //     super.exportRows(ti, onExportListener);
    //     // 启用自动增长。
    //     // this.tableAutoIncrement(ti, true);
    // }

    @Override
    public String getDatabaseVendor() {
        return DatabaseVendorEnum.MICROSOFT_ACCESS.getVendor();
    }

    @Override
    public BasicDatabaseDao getDatabaseDao() {
        return this.databaseDao;
    }

    @Override
    public Class<? extends DatabaseDialect> getDatabaseDialectClass() {
        return AccessDatabaseDialect.class;
    }

    @Override
    protected void buildForeignKeyChangeRules(StringBuilder sBuilder, ImportedKey importedKey) {

    }

    @Override
    public void createAll(List<TableInfo> tis) {
        try {
            this.beforeCreateAll(tis);

            for (TableInfo ti : tis) {
                this.create(ti);
            }

        } catch (Exception e) {
            this.handleException(e);
        }
    }

    @Override
    public DatabaseComparator getComparator() {
        return this.databaseComparator;
    }

}
