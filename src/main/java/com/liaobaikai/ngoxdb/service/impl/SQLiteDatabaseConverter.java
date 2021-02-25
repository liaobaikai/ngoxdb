package com.liaobaikai.ngoxdb.service.impl;

import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
import com.liaobaikai.ngoxdb.config.DatabaseConfig;
import com.liaobaikai.ngoxdb.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.dao.impl.SQLiteDatabaseDao;
import com.liaobaikai.ngoxdb.enums.DatabaseVendorEnum;
import com.liaobaikai.ngoxdb.enums.fn.DatabaseFunctionEnum;
import com.liaobaikai.ngoxdb.info.ColumnInfo;
import com.liaobaikai.ngoxdb.info.TableInfo;
import com.liaobaikai.ngoxdb.service.BasicDatabaseConverter;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * @author baikai.liao
 * @Time 2021-02-25 22:32:37
 */
@Service
public class SQLiteDatabaseConverter extends BasicDatabaseConverter {

    private final SQLiteDatabaseDao databaseDao;

    public SQLiteDatabaseConverter() {
        this.databaseDao = null;
    }

    public SQLiteDatabaseConverter(JdbcTemplate2 jdbcTemplate,
                                   boolean isMaster,
                                   String masterDatabaseVendor,
                                   DatabaseConfig databaseConfig) {
        super(jdbcTemplate, isMaster, masterDatabaseVendor, databaseConfig);
        this.databaseDao = new SQLiteDatabaseDao(jdbcTemplate);
    }

    @Override
    public void buildDatabaseFunctionMap(Map<DatabaseFunctionEnum, String[]> map) {

    }

    @Override
    public void buildIndex(TableInfo ti) {

    }

    @Override
    protected void handleDataType(StringBuilder sqlBuilder, ColumnInfo columnInfo) {

    }

    @Override
    public BasicDatabaseDao getDatabaseDao() {
        return this.databaseDao;
    }

    @Override
    public String getPaginationSQL(TableInfo ti, int offset, int limit) {
        String orderBy = "";
        if(ti.getPrimaryKeys().size() == 1){
            // 获取主键列名
            final String colName = ti.getPrimaryKeys().get(0).getColumnName();
            orderBy = "order by " + colName;
        }

        return "SELECT * FROM " + ti.getTableName() + " " + orderBy + " LIMIT " + limit + " OFFSET " + offset;
    }

    @Override
    public String getDatabaseVendor() {
        return DatabaseVendorEnum.SQLITE.getVendor();
    }

    @Override
    public String getRightName(String name) {
        return name;
    }

    @Override
    public List<String> getUnsupportedFunctions() {
        return null;
    }
}
