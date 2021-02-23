package com.liaobaikai.ngoxdb.service.impl;

import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
import com.liaobaikai.ngoxdb.config.DatabaseConfig;
import com.liaobaikai.ngoxdb.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.dao.impl.AccessDatabaseDao;
import com.liaobaikai.ngoxdb.enums.DatabaseVendorEnum;
import com.liaobaikai.ngoxdb.enums.fn.DatabaseFunctionEnum;
import com.liaobaikai.ngoxdb.enums.fn.DateTypeFunctionEnum;
import com.liaobaikai.ngoxdb.info.ColumnInfo;
import com.liaobaikai.ngoxdb.info.TableInfo;
import com.liaobaikai.ngoxdb.rs.ImportedKey;
import com.liaobaikai.ngoxdb.service.BasicDatabaseConverter;
import com.liaobaikai.ngoxdb.types.MsAccessType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.Types;
import java.util.List;
import java.util.Map;

/**
 * Microsoft Access 转换器
 * @author baikai.liao
 * @Time 2021-01-29 23:42:45
 */
@Slf4j
@Service
public class AccessDatabaseConverter extends BasicDatabaseConverter {

    private final AccessDatabaseDao databaseDao;

    public AccessDatabaseConverter() {
        this.databaseDao = null;
    }

    public AccessDatabaseConverter(JdbcTemplate2 jdbcTemplate,
                                   boolean isMaster,
                                   String masterDatabaseVendor,
                                   DatabaseConfig databaseConfig) {
        super(jdbcTemplate, isMaster, masterDatabaseVendor, databaseConfig);
        this.databaseDao = new AccessDatabaseDao(jdbcTemplate);
        this.truncateMetadata();
    }

    @Override
    public List<String> getUnsupportedFunctions() {
        return null;
    }

    @Override
    public void applySlaveDatabaseMetadata() {

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
    public boolean beforeCreateTable(TableInfo ti) {

        DatabaseConfig databaseConfig = this.getDatabaseDao().getJdbcTemplate().getDatabaseConfig();
        if(databaseConfig.isReplaceTable()){
            // 替换表
            // 查询表是否存在
            if(this.getDatabaseDao().getTableCount(this.isSameDatabaseVendor() ? ti.getTableSchem() : null, ti.getTableName()) > 0){
                log.info("已跳过(ReplaceTable)：表{}已存在！", ti.getTableName());
                return false;
            }
        }
        return true;
    }

    @Override
    public BasicDatabaseDao getDatabaseDao() {
        return this.databaseDao;
    }

    @Override
    public String getPaginationSQL(TableInfo ti, int offset, int limit) {
        return "";
    }

    @Override
    protected void buildForeignKeyChangeRules(StringBuilder sBuilder, ImportedKey importedKey) {
        log.info("Microsoft-Access database not support foreign key on update rule and on delete rule !!!!");
    }

    @Override
    public void buildIndex(TableInfo ti) {

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

    @Override
    protected void handleDataType(StringBuilder sqlBuilder, ColumnInfo columnInfo) {
        MsAccessType accessType = MsAccessType.getByJdbcType(columnInfo.getDataType());
        switch (accessType){

            case NCHAR:
            case CHAR:
                // 长度
                if(columnInfo.getColumnSize() > MsAccessType.CHAR.getPrecision()){
                    sqlBuilder.append(MsAccessType.TEXT.getName());
                } else {
                    sqlBuilder.append(accessType.getName()).append("(").append(columnInfo.getColumnSize()).append(")");
                }
                break;

            case NVARCHAR:
            case VARCHAR:
                if(columnInfo.getColumnSize() > MsAccessType.VARCHAR.getPrecision()){
                    sqlBuilder.append(MsAccessType.TEXT.getName());
                } else {
                    sqlBuilder.append(accessType.getName()).append("(").append(columnInfo.getColumnSize()).append(")");
                }
                break;

            case BINARY:
                if(columnInfo.getColumnSize() > MsAccessType.BINARY.getPrecision()){
                    sqlBuilder.append(MsAccessType.LONGVARBINARY.getName());
                } else {
                    sqlBuilder.append(accessType.getName()).append("(").append(columnInfo.getColumnSize()).append(")");
                }
                break;
            case VARBINARY:
                if(columnInfo.getColumnSize() > MsAccessType.VARBINARY.getPrecision()){
                    sqlBuilder.append(MsAccessType.LONGVARBINARY.getName());
                } else {
                    sqlBuilder.append(accessType.getName()).append("(").append(columnInfo.getColumnSize()).append(")");
                }
                break;

            case DECIMAL:
            case NUMERIC:
                sqlBuilder.append(accessType.getName()).append("(");

                if(columnInfo.getColumnSize() > MsAccessType.DECIMAL.getPrecision() || columnInfo.getColumnSize() == 0){
                    sqlBuilder.append(accessType.getPrecision());
                } else {
                    sqlBuilder.append(columnInfo.getColumnSize());
                }

                if(columnInfo.getDecimalDigits() > MsAccessType.DECIMAL.getPrecision()){
                    columnInfo.setDecimalDigits(MsAccessType.DECIMAL.getPrecision().intValue());
                }

                if(columnInfo.getDecimalDigits() > 0){
                    sqlBuilder.append(", ").append(columnInfo.getDecimalDigits());
                }

                sqlBuilder.append(")");
                break;
            case BOOLEAN:
                sqlBuilder.append(MsAccessType.BIT.getName());
                break;



            case REAL:
            case FLOAT:

            case CLOB:
            case LONGVARCHAR:

            case BLOB:
            case LONGVARBINARY:

            case NCLOB:
            case LONGNVARCHAR:
                // jdbc unicode 不支持

            case TEXT:
            case NTEXT:

            case TIME:
            case TIMESTAMP:
            case DATE:
            case DATETIME:

            case BIT:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                sqlBuilder.append(accessType.getName());
                break;
            case UNKNOWN:
                // 未知的
                // switch (columnInfo.getDataType()){
                //     // case Types.LONGNVARCHAR:
                //     //     // SQLServer NText
                //     //     sqlBuilder.append(MsAccessType.CLOB.getName());
                //     //     break;
                //     // case Types.NCHAR:
                //     //     // NChar
                //     //     if(columnInfo.getColumnSize() > MsAccessType.TEXT.getPrecision()){
                //     //         sqlBuilder.append(MsAccessType.CLOB.getName());
                //     //     } else if(columnInfo.getColumnSize() > MsAccessType.CHAR.getPrecision()){
                //     //         sqlBuilder.append(MsAccessType.TEXT.getName());
                //     //     } else {
                //     //         sqlBuilder.append(MsAccessType.CHAR.getName()).append("(").append(columnInfo.getColumnSize()).append(")");
                //     //     }
                //     //     break;
                //     case Types.BINARY:
                //         if(columnInfo.getColumnSize() > MsAccessType.BINARY.getPrecision()){
                //             sqlBuilder.append(MsAccessType.BLOB.getName());
                //         } else {
                //             sqlBuilder.append(MsAccessType.BINARY.getName()).append("(").append(columnInfo.getColumnSize()).append(")");
                //         }
                //         break;
                //     case Types.VARBINARY:
                //         if(columnInfo.getColumnSize() > MsAccessType.VARBINARY.getPrecision()){
                //             sqlBuilder.append(MsAccessType.BLOB.getName());
                //         } else {
                //             sqlBuilder.append(MsAccessType.VARBINARY.getName()).append("(").append(columnInfo.getColumnSize()).append(")");
                //         }
                //         break;
                //     case Types.LONGVARBINARY:
                //         sqlBuilder.append(MsAccessType.BLOB.getName());
                //         break;
                //
                //     default:
                //         // varbinary
                //
                // }
                System.out.println("unknown:" + columnInfo.getDataType() + ", " + columnInfo.getTypeName());
                return;
            default:
                sqlBuilder.append(columnInfo.getTypeName());
        }
    }

    @Override
    public String getAutoincrement() {
        return "autoincrement";
    }

}
