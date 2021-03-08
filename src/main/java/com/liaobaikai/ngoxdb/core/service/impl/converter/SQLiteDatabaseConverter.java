package com.liaobaikai.ngoxdb.core.service.impl.converter;

import com.liaobaikai.ngoxdb.bean.info.ColumnInfo;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
import com.liaobaikai.ngoxdb.core.config.DatabaseConfig;
import com.liaobaikai.ngoxdb.core.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.core.dao.impl.SQLiteDatabaseDao;
import com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum;
import com.liaobaikai.ngoxdb.core.enums.fn.DatabaseFunctionEnum;
import com.liaobaikai.ngoxdb.core.service.DatabaseComparator;
import com.liaobaikai.ngoxdb.core.service.impl.comparator.SQLiteDatabaseComparator;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

import java.sql.Types;
import java.util.List;
import java.util.Map;

/**
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

    public SQLiteDatabaseConverter(JdbcTemplate2 jdbcTemplate,
                                   boolean isMaster,
                                   String masterDatabaseVendor,
                                   DatabaseConfig databaseConfig) {
        super(jdbcTemplate, isMaster, masterDatabaseVendor, databaseConfig);
        this.databaseDao = new SQLiteDatabaseDao(jdbcTemplate);
        this.databaseComparator = new SQLiteDatabaseComparator(this);
        this.truncateMetadata();
    }

    @Override
    public Logger getLogger() {
        return log;
    }

    @Override
    public void buildDatabaseFunctionMap(Map<DatabaseFunctionEnum, String[]> map) {

    }

    @Override
    public String buildCreateTable(TableInfo ti) {

        StringBuilder sqlBuilder = new StringBuilder("CREATE TABLE ");
        sqlBuilder.append(this.getRightName(ti.getTableName()));
        if (ti.getRemarks() != null) {
            sqlBuilder.append(" /*").append(ti.getRemarks()).append("*/");
        }
        sqlBuilder.append(" ( ");

        // 列信息
        ti.getColumns().forEach(columnInfo -> {

            sqlBuilder.append(getRightName(columnInfo.getColumnName())).append(" ");

            if (this.isSameDatabaseVendor()) {
                // 相同的数据库厂家
                sqlBuilder.append(columnInfo.getColumnType());
            } else {
                // 其他数据库厂家
                this.handleDataType(sqlBuilder, columnInfo);
            }

            sqlBuilder.append(" ");
            if (columnInfo.isNotNull()) {
                sqlBuilder.append("NOT NULL ");
            }

            // 默认值
            if (columnInfo.getColumnDef() != null) {
                // 时间默认值
                String dataTypeDef = this.getDatabaseDataTypeDefault(columnInfo.getColumnDef(), columnInfo.getDataType());
                sqlBuilder.append("DEFAULT ").append(dataTypeDef == null ? columnInfo.getColumnDef() : dataTypeDef).append(" ");
            }

            sqlBuilder.append(",");

            if (columnInfo.getRemarks() != null) {
                // 添加语句注释
                sqlBuilder.append(" /*").append(columnInfo.getRemarks()).append("*/");
            }

        });

        // 主键
        buildPrimaryKeys(ti, sqlBuilder);
        // 约束
        buildConstraintInfo(ti, sqlBuilder);

        sqlBuilder.delete(sqlBuilder.length() - 1, sqlBuilder.length()).append(") ");

        return sqlBuilder.toString();
    }

    @Override
    public void buildComment(TableInfo ti) {
        // 不支持添加注释
    }

    @Override
    protected void handleDataType(StringBuilder sqlBuilder, ColumnInfo columnInfo) {

        String typeName = "";   // 类型名称
        int length = -1;        // 字符长度
        int precision = -1;     // 精度
        int scale = -1;         // 刻度

        switch (columnInfo.getDataType()) {
            case Types.BIT:
                typeName = "bit";
                break;
            case Types.TINYINT:
                break;
            case Types.SMALLINT:
                break;
            case Types.INTEGER:
                break;
            case Types.BIGINT:
                break;
            case Types.FLOAT:
                break;
            case Types.REAL:
                break;
            case Types.DOUBLE:
                break;
            case Types.NUMERIC:
                break;
            case Types.DECIMAL:
                break;
            case Types.CHAR:
                break;
            case Types.VARCHAR:
                break;
            case Types.LONGVARCHAR:
                break;
            case Types.DATE:
                break;
            case Types.TIME:
                break;
            case Types.TIMESTAMP:
                break;
            case Types.BINARY:
                break;
            case Types.VARBINARY:
                break;
            case Types.LONGVARBINARY:
                break;
            case Types.NULL:
                break;
            case Types.OTHER:
                break;
            case Types.JAVA_OBJECT:
                break;
            case Types.DISTINCT:
                break;
            case Types.STRUCT:
                break;
            case Types.ARRAY:
                break;
            case Types.BLOB:
                break;
            case Types.CLOB:
                break;
            case Types.REF:
                break;
            case Types.DATALINK:
                break;
            case Types.BOOLEAN:
                break;
            case Types.ROWID:
                break;
            case Types.NCHAR:
                break;
            case Types.NVARCHAR:
                break;
            case Types.LONGNVARCHAR:
                break;
            case Types.NCLOB:
                break;
            case Types.SQLXML:
                break;
            case Types.REF_CURSOR:
                break;
            case Types.TIME_WITH_TIMEZONE:
                break;
            case Types.TIMESTAMP_WITH_TIMEZONE:
                break;
        }
    }

    @Override
    public BasicDatabaseDao getDatabaseDao() {
        return this.databaseDao;
    }

    @Override
    public String getPaginationSQL(TableInfo ti, int offset, int limit) {
        // 唯一键
        String colNames = this.getTableUniqueKeys(ti.getUniqueKeys());
        if (colNames.length() == 0) {
            // 无唯一键
            // 使用全部列
            colNames = this.getTableOrderColumnNames(ti.getColumns());
        }

        return "SELECT * FROM " + ti.getTableName() + " ORDER BY " + colNames + " LIMIT " + limit + " OFFSET " + offset;
    }

    @Override
    public String getDatabaseVendor() {
        return DatabaseVendorEnum.SQLITE.getVendor();
    }

    @Override
    public String getRightName(String name) {
        // 可支持多种，``, "", '', []
        return "'" + name + "'";
    }

    @Override
    public List<String> getUnsupportedFunctions() {
        return null;
    }

    @Override
    public DatabaseComparator getComparator() {
        return this.databaseComparator;
    }


}
