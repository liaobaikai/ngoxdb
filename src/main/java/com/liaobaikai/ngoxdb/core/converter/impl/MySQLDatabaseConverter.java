package com.liaobaikai.ngoxdb.core.converter.impl;

import com.liaobaikai.ngoxdb.bean.NgoxDbMaster;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.bean.rs.ImportedKey;
import com.liaobaikai.ngoxdb.core.comparator.DatabaseComparator;
import com.liaobaikai.ngoxdb.core.comparator.impl.MySQLDatabaseComparator;
import com.liaobaikai.ngoxdb.core.converter.BasicDatabaseConverter;
import com.liaobaikai.ngoxdb.core.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.core.dao.impl.MySQLDatabaseDao;
import com.liaobaikai.ngoxdb.core.dialect.DatabaseDialect;
import com.liaobaikai.ngoxdb.core.dialect.impl.MySQLDatabaseDialect;
import com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.List;

/**
 * MySQL 转换器
 *
 * @author baikai.liao
 * @Time 2021-01-28 15:54:44
 */
@Slf4j
@Service
public class MySQLDatabaseConverter extends BasicDatabaseConverter {

    protected final MySQLDatabaseDao databaseDao;
    private final MySQLDatabaseComparator databaseComparator;

    public MySQLDatabaseConverter() {
        this.databaseDao = null;
        this.databaseComparator = null;
    }

    public MySQLDatabaseConverter(NgoxDbMaster ngoxDbMaster) {
        super(ngoxDbMaster);
        this.databaseDao = new MySQLDatabaseDao(ngoxDbMaster);
        this.databaseComparator = new MySQLDatabaseComparator(this);
    }

    @Override
    public Logger getLogger() {
        return log;
    }

    @Override
    public boolean isSameDatabaseVendor() {
        if (this.getNgoxDbMaster().isMaster()) {
            return false;
        }
        return DatabaseVendorEnum.isMySQLFamily(this.getNgoxDbMaster().getMasterDatabaseVendor()) && DatabaseVendorEnum.isMySQLFamily(this.getDatabaseVendor());
    }

    @Override
    public String buildCreateTable(TableInfo ti) {
        StringBuilder sBuilder = new StringBuilder(super.buildCreateTable(ti));

        sBuilder.append(" ");
        // 自动增长
        if (ti.getAutoIncrement() != null) {
            sBuilder.append(this.getDatabaseDialect().getIdentityColumnString())
                    .append("=")
                    .append(ti.getAutoIncrement())
                    .append(" ");
        }

        // 字符集
        if (isSameDatabaseVendor() && ti.getTableCollation() != null) {
            sBuilder.append(this.getDatabaseDialect().getCollationString(ti.getTableCollation()))
                    .append(" ");
        }

        // 表注释
        if (this.getDatabaseDialect().supportsCommentOnBuildTable()
                && StringUtils.isNotEmpty(ti.getRemarks())) {
            sBuilder.append(this.getDatabaseDialect().getTableCommentString(getFinalTableName(ti.getTableName()), ti.getRemarks()))
                    .append(" ");
        }

        return sBuilder.toString();
    }

    @Override
    protected void buildForeignKeyChangeRules(StringBuilder sBuilder, ImportedKey importedKey) {
        // 删除规则
        // As of NDB 8.0.16: For NDB tables, ON DELETE CASCADE is not supported where the child table contains one or more columns of any of the TEXT or BLOB types. (Bug #89511, Bug #27484882)
        // https://dev.mysql.com/doc/refman/8.0/en/create-table-foreign-keys.html
        // For an ON DELETE or ON UPDATE that is not specified, the default action is always NO ACTION.
        if (importedKey.getDeleteRule() == ImportedKey.SET_DEFAULT) {
            importedKey.setDeleteRule(ImportedKey.NO_ACTION);
        }
        sBuilder.append(" ").append(importedKey.getDeleteAction()).append(" ");

        // 更新规则
        // For an ON DELETE or ON UPDATE that is not specified, the default action is always NO ACTION.
        if (importedKey.getUpdateRule() == ImportedKey.SET_DEFAULT) {
            importedKey.setUpdateRule(ImportedKey.NO_ACTION);
        }
        sBuilder.append(" ").append(importedKey.getUpdateAction()).append(" ");
    }

    @Override
    public BasicDatabaseDao getDatabaseDao() {
        return this.databaseDao;
    }

    @Override
    public Class<? extends DatabaseDialect> getDatabaseDialectClass() {
        return MySQLDatabaseDialect.class;
    }

    @Override
    public String getDatabaseVendor() {
        return DatabaseVendorEnum.MYSQL.getVendor();
    }

    @Override
    public void beforeImportRows(Connection con, List<Object[]> batchArgs, TableInfo ti) {

        // timestamp仅支持大于1000, 1000指1秒
        // 其他数据库可以存储小于0的timestamp.
        // 否则会抛出错误：
        // Data truncation: Incorrect datetime value: '1900-01-01 00:00:00' for column
        // Data truncation: Incorrect datetime value: '1970-01-01 08:00:00' for column

        for (Object[] objects : batchArgs) {
            // row
            for (int x = 0, len = objects.length; x < len; x++) {
                // cell
                if (objects[x] instanceof Timestamp
                        && ((Timestamp) objects[x]).before(new Timestamp(1000))) {
                    objects[x] = new Timestamp(1000);
                }
            }
        }
    }

    @Override
    public DatabaseComparator getComparator() {
        return this.databaseComparator;
    }


}
