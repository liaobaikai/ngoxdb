package com.liaobaikai.ngoxdb.core.service.impl.converter;

import com.liaobaikai.ngoxdb.bean.info.ColumnInfo;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.bean.rs.ImportedKey;
import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
import com.liaobaikai.ngoxdb.core.config.DatabaseConfig;
import com.liaobaikai.ngoxdb.core.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.core.dao.impl.AccessDatabaseDao;
import com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum;
import com.liaobaikai.ngoxdb.core.enums.fn.DatabaseFunctionEnum;
import com.liaobaikai.ngoxdb.core.enums.fn.DateTypeFunctionEnum;
import com.liaobaikai.ngoxdb.core.listener.OnMigrateTableDataListener;
import com.liaobaikai.ngoxdb.core.service.DatabaseComparator;
import com.liaobaikai.ngoxdb.core.service.impl.comparator.AccessDatabaseComparator;
import com.liaobaikai.ngoxdb.utils.DateUtils;
import com.liaobaikai.ngoxdb.utils.StringUtils;
import lombok.extern.slf4j.Slf4j;
import net.ucanaccess.jdbc.UcanaccessDatabaseMetadata;
import org.hsqldb.types.ClobInputStream;
import org.slf4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.*;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

import java.io.StringReader;
import java.io.StringWriter;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

/**
 * Microsoft Access 转换器
 *
 * @author baikai.liao
 * @Time 2021-01-29 23:42:45
 */
@Slf4j
@Service
public class AccessDatabaseConverter extends BasicDatabaseConverter {

    /**
     * 字符串最大宽度
     */
    public static final int CHAR_MAX_SIZE = 255;
    public static final int BYTE_MAX_SIZE = 255;

    private final AccessDatabaseDao databaseDao;
    private final AccessDatabaseComparator databaseComparator;
    // 是否为空表
    private long tableRowCount;

    private final static SimpleDateFormat DATETIME_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public AccessDatabaseConverter() {
        this.databaseDao = null;
        this.databaseComparator = null;
    }

    public AccessDatabaseConverter(JdbcTemplate2 jdbcTemplate,
                                   boolean isMaster,
                                   String masterDatabaseVendor,
                                   DatabaseConfig databaseConfig) {
        super(jdbcTemplate, isMaster, masterDatabaseVendor, databaseConfig);
        // drop table command is invalid!!!!
        this.getLogger().warn("Drop table command is not compatible!!! Parameter {replace-table} always false!");
        databaseConfig.setReplaceTable(false);
        this.databaseDao = new AccessDatabaseDao(jdbcTemplate);
        this.databaseComparator = new AccessDatabaseComparator(this);
        this.truncateMetadata();
    }

    @Override
    public List<String> getUnsupportedFunctions() {
        return null;
    }

    @Override
    public Logger getLogger() {
        return log;
    }

    @Override
    public void buildDatabaseFunctionMap(Map<DatabaseFunctionEnum, String[]> map) {

        // http://ucanaccess.sourceforge.net/site.html#examples
        //

        // map.put(MathematicalFunctionEnum.abs, new String[]{""});
        // map.put(MathematicalFunctionEnum.cbrt, new String[]{""});
        // map.put(MathematicalFunctionEnum.ceil, new String[]{""});
        // map.put(MathematicalFunctionEnum.ceiling, new String[]{""});
        // map.put(MathematicalFunctionEnum.degrees, new String[]{""});
        // map.put(MathematicalFunctionEnum.div, new String[]{""});
        // map.put(MathematicalFunctionEnum.exp, new String[]{""});
        // map.put(MathematicalFunctionEnum.factorial, new String[]{""});
        // map.put(MathematicalFunctionEnum.floor, new String[]{""});
        // map.put(MathematicalFunctionEnum.gcd, new String[]{""});
        // map.put(MathematicalFunctionEnum.lcm, new String[]{""});
        // map.put(MathematicalFunctionEnum.ln, new String[]{""});
        // map.put(MathematicalFunctionEnum.log, new String[]{""});
        // map.put(MathematicalFunctionEnum.log10, new String[]{""});
        // map.put(MathematicalFunctionEnum.min_scale, new String[]{""});
        // map.put(MathematicalFunctionEnum.mod, new String[]{""});
        // map.put(MathematicalFunctionEnum.pi, new String[]{""});
        // map.put(MathematicalFunctionEnum.power, new String[]{""});
        // map.put(MathematicalFunctionEnum.radians, new String[]{""});
        // map.put(MathematicalFunctionEnum.round, new String[]{""});
        // map.put(MathematicalFunctionEnum.scale, new String[]{""});
        // map.put(MathematicalFunctionEnum.sign, new String[]{""});
        // map.put(MathematicalFunctionEnum.sqrt, new String[]{""});
        // map.put(MathematicalFunctionEnum.trim_scale, new String[]{""});
        // map.put(MathematicalFunctionEnum.trunc, new String[]{""});
        // map.put(MathematicalFunctionEnum.width_bucket, new String[]{""});
        // map.put(MathematicalFunctionEnum.random, new String[]{""});
        // map.put(MathematicalFunctionEnum.acos, new String[]{""});
        // map.put(MathematicalFunctionEnum.acosd, new String[]{""});
        // map.put(MathematicalFunctionEnum.asin, new String[]{""});
        // map.put(MathematicalFunctionEnum.asind, new String[]{""});
        // map.put(MathematicalFunctionEnum.atan, new String[]{""});
        // map.put(MathematicalFunctionEnum.atand, new String[]{""});
        // map.put(MathematicalFunctionEnum.atan2, new String[]{""});
        // map.put(MathematicalFunctionEnum.atan2d, new String[]{""});
        // map.put(MathematicalFunctionEnum.cos, new String[]{""});
        // map.put(MathematicalFunctionEnum.cosd, new String[]{""});
        // map.put(MathematicalFunctionEnum.cot, new String[]{""});
        // map.put(MathematicalFunctionEnum.cotd, new String[]{""});
        // map.put(MathematicalFunctionEnum.sin, new String[]{""});
        // map.put(MathematicalFunctionEnum.sind, new String[]{""});
        // map.put(MathematicalFunctionEnum.tan, new String[]{""});
        // map.put(MathematicalFunctionEnum.tand, new String[]{""});


        // map.put(DateTypeFunctionEnum.to_char, new String[]{""});
        // map.put(DateTypeFunctionEnum.to_date, new String[]{""});
        // map.put(DateTypeFunctionEnum.to_number, new String[]{""});
        // map.put(DateTypeFunctionEnum.to_timestamp, new String[]{""});

        // http://ucanaccess.sourceforge.net/site.html#examples
        map.put(DateTypeFunctionEnum.age, new String[]{"DATEDIFF"});
        map.put(DateTypeFunctionEnum.clock_timestamp, new String[]{"Now()"});    // Current date and time (changes during statement execution);
        map.put(DateTypeFunctionEnum.current_date, new String[]{"Now()"});
        map.put(DateTypeFunctionEnum.current_time, new String[]{"Now()"});
        map.put(DateTypeFunctionEnum.current_timestamp, new String[]{"Now()"});
        map.put(DateTypeFunctionEnum.date_part, new String[]{"DATEPART"});
        // map.put(DateTypeFunctionEnum.date_trunc, new String[]{""});
        // map.put(DateTypeFunctionEnum.extract, new String[]{""});
        // map.put(DateTypeFunctionEnum.isfinite, new String[]{""});
        // map.put(DateTypeFunctionEnum.justify_days, new String[]{""});
        // map.put(DateTypeFunctionEnum.justify_hours, new String[]{""});
        // map.put(DateTypeFunctionEnum.justify_interval, new String[]{""});
        map.put(DateTypeFunctionEnum.localtime, new String[]{"Now()"});
        map.put(DateTypeFunctionEnum.localtimestamp, new String[]{"Now()"});
        // map.put(DateTypeFunctionEnum.make_date, new String[]{""});
        // map.put(DateTypeFunctionEnum.make_interval, new String[]{""});
        // map.put(DateTypeFunctionEnum.make_time, new String[]{""});
        // map.put(DateTypeFunctionEnum.make_timestamp, new String[]{""});
        // map.put(DateTypeFunctionEnum.make_timestamptz, new String[]{""});
        map.put(DateTypeFunctionEnum.now, new String[]{"Now()"});    // Current date and time (start of current transaction);
        map.put(DateTypeFunctionEnum.statement_timestamp, new String[]{"Now()"});    // Current date and time (start of current statement);
        map.put(DateTypeFunctionEnum.timeofday, new String[]{"Now()"});
        map.put(DateTypeFunctionEnum.transaction_timestamp, new String[]{"Now()"});  // Current date and time (start of current transaction);


        // map.put(StringFunctionEnum.bit_length, new String[]{""});
        // map.put(StringFunctionEnum.char_length, new String[]{""});
        // map.put(StringFunctionEnum.character_length, new String[]{""});
        // map.put(StringFunctionEnum.lower, new String[]{""});
        // map.put(StringFunctionEnum.normalize, new String[]{""});
        // map.put(StringFunctionEnum.octet_length, new String[]{""});
        // map.put(StringFunctionEnum.overlay, new String[]{""});
        // map.put(StringFunctionEnum.position, new String[]{""});
        // map.put(StringFunctionEnum.substring, new String[]{""});
        // map.put(StringFunctionEnum.trim, new String[]{""});
        // map.put(StringFunctionEnum.upper, new String[]{""});
        // map.put(StringFunctionEnum.ascii, new String[]{""});
        // map.put(StringFunctionEnum.btrim, new String[]{""});
        // map.put(StringFunctionEnum.chr, new String[]{""});
        // map.put(StringFunctionEnum.concat, new String[]{""});
        // map.put(StringFunctionEnum.concat_ws, new String[]{""});
        // map.put(StringFunctionEnum.format, new String[]{""});
        // map.put(StringFunctionEnum.initcap, new String[]{""});
        // map.put(StringFunctionEnum.left, new String[]{""});
        // map.put(StringFunctionEnum.length, new String[]{""});
        // map.put(StringFunctionEnum.lpad, new String[]{""});
        // map.put(StringFunctionEnum.ltrim, new String[]{""});
        // map.put(StringFunctionEnum.md5, new String[]{""});
        // map.put(StringFunctionEnum.parse_ident, new String[]{""});
        // map.put(StringFunctionEnum.quote_ident, new String[]{""});
        // map.put(StringFunctionEnum.quote_literal, new String[]{""});
        // map.put(StringFunctionEnum.quote_nullable, new String[]{""});
        // map.put(StringFunctionEnum.regexp_match, new String[]{""});
        // map.put(StringFunctionEnum.regexp_matches, new String[]{""});
        // map.put(StringFunctionEnum.regexp_replace, new String[]{""});
        // map.put(StringFunctionEnum.regexp_split_to_array, new String[]{""});
        // map.put(StringFunctionEnum.regexp_split_to_table, new String[]{""});
        // map.put(StringFunctionEnum.repeat, new String[]{""});
        // map.put(StringFunctionEnum.replace, new String[]{""});
        // map.put(StringFunctionEnum.reverse, new String[]{""});
        // map.put(StringFunctionEnum.right, new String[]{""});
        // map.put(StringFunctionEnum.rpad, new String[]{""});
        // map.put(StringFunctionEnum.rtrim, new String[]{""});
        // map.put(StringFunctionEnum.split_part, new String[]{""});
        // map.put(StringFunctionEnum.strpos, new String[]{""});
        // map.put(StringFunctionEnum.substr, new String[]{""});
        // map.put(StringFunctionEnum.starts_with, new String[]{""});
        // map.put(StringFunctionEnum.to_ascii, new String[]{""});
        // map.put(StringFunctionEnum.to_hex, new String[]{""});
        // map.put(StringFunctionEnum.translate, new String[]{""});
        // map.put(StringFunctionEnum.convert, new String[]{""});
        // map.put(StringFunctionEnum.convert_from, new String[]{""});
        // map.put(StringFunctionEnum.convert_to, new String[]{""});
        // map.put(StringFunctionEnum.encode, new String[]{""});
        // map.put(StringFunctionEnum.decode, new String[]{""});
        // map.put(StringFunctionEnum.uuid, new String[]{""});

    }

    @Override
    public void beforeMigrateTableData(TableInfo ti){

        this.tableRowCount = this.getDatabaseDao().getTableRowCount(this.getDatabaseDao().getCatalog(), this.getRightName(ti.getTableName()));
        if(tableRowCount > 0){
            this.getLogger().info("[{}] - {migrateTableData} - Skip table [{}]", this.getDatabaseConfig().getName(), ti.getTableName());
            return;
        }

        // https://stackoverflow.com/questions/44420840/change-autonumber-values-when-inserting-rows-with-ucanaccess/44424112#44424112
        // 3.0.0 Release
        // It may be useful in import/export of data from and to different tables with the same structure, avoiding to break some FK constraint.
        this.tableAutoIncrement(ti, false);
    }

    @Override
    public void postData(List<Object[]> batchArgs, TableInfo ti) {
        if(tableRowCount > 0){
            this.getLogger().info("[{}] - {migrateTableData} - Table [{}] skip import {} rows.",
                    this.getDatabaseConfig().getName(), ti.getTableName(), batchArgs.size());
            return;
        }

        // https://stackoverflow.com/questions/44420840/change-autonumber-values-when-inserting-rows-with-ucanaccess/44424112#44424112
        // 3.0.0 Release
        // It may be useful in import/export of data from and to different tables with the same structure, avoiding to break some FK constraint.
        super.postData(batchArgs, ti);
    }

    @Override
    public void afterMigrateTableData(TableInfo ti, long tablePageSize) {
        // 启用自动增长
        if(tableRowCount > 0){
            return;
        }
        this.tableAutoIncrement(ti, true);
    }

    // @Override
    // protected String buildInsertSQL(TableInfo ti) {
    //
    //     // 生成sql语句：
    //     // insert into T (col1, col2, col3, ...) values (?, ?, ?, ...)
    //     final StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ").append(this.getRightName(ti.getTableName())).append(" (");
    //
    //     for (ColumnInfo c : ti.getColumns()) {
    //         sqlBuilder.append(this.getRightName(c.getColumnName())).append(",");
    //     }
    //
    //     sqlBuilder.delete(sqlBuilder.length() - 1, sqlBuilder.length()).append(") VALUES (");
    //     sqlBuilder.append("%s").append(")");
    //     return sqlBuilder.toString();
    //
    // }
    //
    // @Override
    // public void persistent(final String sqlTemplate, List<Object[]> batchArgs, TableInfo ti) {
    //
    //     StringBuilder valueBuilder = new StringBuilder();
    //     int insertCount = 0;
    //     for(Object[] rowValues: batchArgs) {
    //
    //         for(int i = 0, len = rowValues.length; i < len; i++){
    //
    //             Object rowValue = rowValues[i];
    //
    //             // 判断是否为byte
    //
    //
    //             if(StringUtils.isStringValue(rowValue.getClass())){
    //                 // String
    //                 valueBuilder.append("'").append(rowValue).append("'");
    //             } else if(DateUtils.isDateValue(rowValue.getClass())){
    //                 // 转换为字符串
    //                 valueBuilder.append("'").append(DATETIME_FORMATTER.format(((java.util.Date) rowValue))).append("'");
    //             } else {
    //                 // 无需双引号
    //                 valueBuilder.append(rowValue);
    //             }
    //
    //             if(i != len - 1){
    //                 valueBuilder.append(",");
    //             }
    //
    //         }
    //
    //         String finalSql = String.format(sqlTemplate, valueBuilder.toString());
    //         insertCount += this.getDatabaseDao().getJdbcTemplate().update(finalSql);
    //
    //         valueBuilder.delete(0, valueBuilder.length());
    //     }
    //
    //     System.out.println(insertCount);
    //
    // }


    // @Override
    // public void changeBatchArgs(List<Object[]> batchArgs, TableInfo ti) {
    //     for(Object[] rowValues: batchArgs){
    //         for(int i = 0, len = rowValues.length; i < len; i++){
    //             ColumnInfo columnInfo = ti.getColumns().get(i);
    //             switch (ti.getColumns().get(i).getDataType()){
    //                 case Types.CHAR:
    //                 case Types.NCHAR:
    //                 case Types.VARCHAR:
    //                 case Types.NVARCHAR:
    //                     // 所有都是文本类型
    //                     if (columnInfo.getColumnSize() > CHAR_MAX_SIZE) {
    //                         // 长文本类型
    //                         try {
    //                             rowValues[i] = new javax.sql.rowset.serial.SerialClob(String.valueOf(rowValues[i]).toCharArray());
    //                         } catch (SQLException ignored) {
    //                         }
    //                         break;
    //                     }
    //                     break;
    //                 case Types.LONGVARCHAR:
    //                 case Types.LONGNVARCHAR:
    //                 case Types.CLOB:
    //                 case Types.NCLOB:
    //                     try {
    //                         rowValues[i] = new javax.sql.rowset.serial.SerialClob(String.valueOf(rowValues[i]).toCharArray());
    //                     } catch (SQLException ignored) {
    //                     }
    //                     break;
    //                 default:
    //                     break;
    //             }
    //         }
    //     }
    // }

    /**
     * 表启用/禁用自动增长
     * @param ti 表信息
     * @param enable true:启用 false:禁用
     */
    private void tableAutoIncrement(TableInfo ti, boolean enable){
        // https://stackoverflow.com/questions/44420840/change-autonumber-values-when-inserting-rows-with-ucanaccess/44424112#44424112
        // 3.0.0 Release
        // It may be useful in import/export of data from and to different tables with the same structure, avoiding to break some FK constraint.
        boolean hasAutoInc = false;
        for(ColumnInfo columnInfo: ti.getColumns()){
            if(columnInfo.isAutoincrement()){
                hasAutoInc = true;
                break;
            }
        }
        if(hasAutoInc) {
            // http://ucanaccess.sourceforge.net/site.html#examples
            // "DISABLE/ENABLE AUTOINCREMENT ON TABLE_NAME"
            String ddl = (enable ? "ENABLE" : "DISABLE") + " AUTOINCREMENT ON " + this.getRightName(ti.getTableName());
            this.getLogger().info("[{}] {}", this.getDatabaseConfig().getName(), ddl);
            this.databaseDao.getJdbcTemplate().execute(ddl);
        }
    }

    @Override
    public void migrateTableData(TableInfo ti, OnMigrateTableDataListener migrateTableDataListener) {
        // 禁用自动增长，不让返回的数据会从1按顺序返回，导致数据错误。
        this.tableAutoIncrement(ti, false);
        // 迁移数据。
        super.migrateTableData(ti, migrateTableDataListener);
        // 启用自动增长。
        this.tableAutoIncrement(ti, true);
    }

    @Override
    public String getDatabaseVendor() {
        return DatabaseVendorEnum.MICROSOFT_ACCESS.getVendor();
    }

    @Override
    public String getRightName(String name) {
        return "[" + name + "]";
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

        return String.format("SELECT * FROM %s ORDER BY %s LIMIT %s, %s",
                this.getRightName(ti.getTableName()),
                colNames,
                offset, limit);
    }

    @Override
    public void buildComment(TableInfo ti) {

    }

    @Override
    public List<ColumnInfo> getColumns(String... tableName) {
        List<ColumnInfo> columns = super.getColumns(tableName);
        columns.forEach(columnInfo -> {
            StringBuilder columnType = new StringBuilder();
            this.handleDataType(columnType, columnInfo);
            columnInfo.setColumnType(columnType.toString());
        });
        return columns;
    }

    /**
     * 数据类型转换
     * {@link net.ucanaccess.converters.TypesMap}
     */
    @Override
    protected void handleDataType(StringBuilder sqlBuilder, ColumnInfo columnInfo) {

        // Access data types: YESNO, BYTE, INTEGER, LONG, SINGLE, DOUBLE, NUMERIC, CURRENCY, COUNTER, TEXT, OLE, MEMO, GUID, DATETIME

        String typeName = "";   // 类型名称
        int length = -1;        // 字符长度
        int precision = -1;     // 精度
        int scale = -1;         // 刻度

        switch (columnInfo.getDataType()) {
            case Types.BIT:
            case Types.BOOLEAN:
                typeName = "YESNO";
                break;
            case Types.TINYINT:
                // 不支持。转换为int
                // typeName = "tinyint";
                // break;
            case Types.SMALLINT:
                // 不支持。转换为int
                // typeName = "smallint";
                // break;
            case Types.INTEGER:
                typeName = "INTEGER";
                break;
            case Types.BIGINT:
                typeName = "LONG";
                break;
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.REAL:
                typeName = "DOUBLE";
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                typeName = "NUMERIC";
                // Precision must be from 1 to 28 inclusive, found 38(Column=A)
                precision = Math.min(columnInfo.getColumnSize(), 28);
                // Scale must be from 0 to 28 inclusive, found 32(Column=A)
                if (columnInfo.getDecimalDigits() > 0) {
                    scale = Math.min(columnInfo.getDecimalDigits(), 28);
                } else {
                    /* ucanaccess 不支持 tinyint
                    if (precision == 3) {
                        typeName = "tinyint";
                        precision = -1;
                    } else if (precision == 5) {
                        typeName = "smallint";
                        precision = -1;
                    }*/
                    if (precision <= 10) {
                        typeName = "integer";
                        precision = -1;
                    } else if (precision == 19) {
                        typeName = "bigint";
                        precision = -1;
                    }
                }
                break;
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.NVARCHAR:
                // 所有都是文本类型
                if (columnInfo.getColumnSize() > CHAR_MAX_SIZE) {
                    // 长文本类型
                    // http://ucanaccess.sourceforge.net/site.html#home
                    typeName = "MEMO";
                    // length = columnInfo.getColumnSize();
                    break;
                }
                typeName = "TEXT";
                break;
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
                // http://ucanaccess.sourceforge.net/site.html#home
                typeName = "MEMO";
                // length = columnInfo.getColumnSize();
                break;
            case Types.DATE:
                typeName = "DATE";
                break;
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                typeName = "TIME";
                break;
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                typeName = "timestamp";
                break;
            case Types.BINARY:
            case Types.VARBINARY:
                if (columnInfo.getColumnSize() > BYTE_MAX_SIZE) {
                    typeName = "BLOB";
                    break;
                }
                typeName = "BYTE";
                length = columnInfo.getColumnSize();
                break;
            case Types.LONGVARBINARY:
            case Types.BLOB:
                typeName = "BLOB";
                break;
            // case Types.NULL:
            //     break;
            // case Types.OTHER:
            //     break;
            // case Types.JAVA_OBJECT:
            //     break;
            // case Types.DISTINCT:
            //     break;
            // case Types.STRUCT:
            //     break;
            // case Types.ARRAY:
            //     typeName = "array";
            //     break;
            // case Types.REF:
            //     break;
            // case Types.DATALINK:
            //     break;

            // case Types.ROWID:
            //     break;
            // case Types.SQLXML:
            //     break;
            // case Types.REF_CURSOR:
            //     break;
        }

        sqlBuilder.append(typeName);
        if (length != -1) {
            sqlBuilder.append("(").append(length).append(")");
        } else if (precision != -1) {
            // 数字精度、刻度
            sqlBuilder.append("(").append(precision);
            if (scale != -1) {
                sqlBuilder.append(", ").append(scale);
            }
            sqlBuilder.append(")");
        }

    }

    @Override
    public String buildCreateTable(TableInfo ti) {

        StringBuilder sqlBuilder = new StringBuilder("CREATE TABLE ");
        sqlBuilder.append(this.getRightName(ti.getTableName())).append(" (");

        // 列信息
        ti.getColumns().forEach(columnInfo -> {

            sqlBuilder.append(getRightName(columnInfo.getColumnName())).append(" ");

            if (columnInfo.isAutoincrement()) {
                sqlBuilder.append(" ").append(this.getAutoincrement()).append(" ");
            } else {
                if (this.isSameDatabaseVendor()) {
                    // 相同的数据库厂家
                    sqlBuilder.append(columnInfo.getColumnType());
                } else {
                    // 其他数据库厂家
                    this.handleDataType(sqlBuilder, columnInfo);
                }
            }


            sqlBuilder.append(" ");

            // 默认值
            if (columnInfo.getColumnDef() != null) {
                // 时间默认值
                String dataTypeDef = this.getDatabaseDataTypeDefault(columnInfo.getColumnDef(), columnInfo.getDataType());
                sqlBuilder.append("DEFAULT ").append(dataTypeDef == null ? columnInfo.getColumnDef() : dataTypeDef).append(" ");
            }

            if (columnInfo.isNotNull()) {
                sqlBuilder.append("NOT NULL ");
            }

            sqlBuilder.append(",");

        });

        // 主键
        buildPrimaryKeys(ti, sqlBuilder);
        // 约束
        buildConstraintInfo(ti, sqlBuilder);

        sqlBuilder.delete(sqlBuilder.length() - 1, sqlBuilder.length()).append(") ");

        return sqlBuilder.toString();
    }



    @Override
    protected void buildForeignKeyChangeRules(StringBuilder sBuilder, ImportedKey importedKey) {

    }

    @Override
    public String getAutoincrement() {
        return "autoincrement";
    }

    @Override
    public DatabaseComparator getComparator() {
        return this.databaseComparator;
    }

}
