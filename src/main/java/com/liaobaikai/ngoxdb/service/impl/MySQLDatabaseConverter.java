package com.liaobaikai.ngoxdb.service.impl;

import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
import com.liaobaikai.ngoxdb.config.DatabaseConfig;
import com.liaobaikai.ngoxdb.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.dao.impl.MySQLDatabaseDao;
import com.liaobaikai.ngoxdb.entity.SlaveMetaDataEntity;
import com.liaobaikai.ngoxdb.enums.DatabaseVendorEnum;
import com.liaobaikai.ngoxdb.enums.IndexTypeEnum;
import com.liaobaikai.ngoxdb.enums.fn.DatabaseFunctionEnum;
import com.liaobaikai.ngoxdb.enums.fn.DateTypeFunctionEnum;
import com.liaobaikai.ngoxdb.enums.fn.MathematicalFunctionEnum;
import com.liaobaikai.ngoxdb.enums.fn.StringFunctionEnum;
import com.liaobaikai.ngoxdb.info.*;
import com.liaobaikai.ngoxdb.rs.ImportedKey;
import com.liaobaikai.ngoxdb.service.BasicDatabaseConverter;
import com.mysql.cj.MysqlType;
import com.sun.org.apache.bcel.internal.generic.BREAKPOINT;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * MySQL 转换器
 * @author baikai.liao
 * @Time 2021-01-28 15:54:44
 */
@Slf4j
@Service
public class MySQLDatabaseConverter extends BasicDatabaseConverter {

    protected final MySQLDatabaseDao databaseDao;

    public MySQLDatabaseConverter() {
        databaseDao = null;
    }

    public MySQLDatabaseConverter(JdbcTemplate2 jdbcTemplate,
                                  boolean isMaster,
                                  String masterDatabaseVendor,
                                  DatabaseConfig databaseConfig) {
        super(jdbcTemplate, isMaster, masterDatabaseVendor, databaseConfig);
        this.databaseDao = new MySQLDatabaseDao(jdbcTemplate);
        this.truncateMetadata();
    }

    @Override
    public boolean isSameDatabaseVendor() {
        if (this.isMaster()) {
            return false;
        }

        return (DatabaseVendorEnum.isMySQL(this.getMasterDatabaseVendor()) || DatabaseVendorEnum.isMariadb(this.getMasterDatabaseVendor()))
                && (DatabaseVendorEnum.isMySQL(this.getDatabaseVendor()) || DatabaseVendorEnum.isMariadb(this.getDatabaseVendor()));
    }

    @Override
    public StringBuilder buildCreateTable(TableInfo ti) {

        StringBuilder sqlBuilder = new StringBuilder(
                "CREATE TABLE " + this.getRightName(ti.getTableName()) + " ( "
        );

        // 列信息
        ti.getColumns().forEach(columnInfo -> {
            sqlBuilder.append(getRightName(columnInfo.getColumnName())).append(" ");
            if (this.isSameDatabaseVendor()) {
                // 数据库厂家相同
                sqlBuilder.append(columnInfo.getColumnType());
            } else {
                // 数据库厂家不同
                // 处理字符串、数字、日期的长度、精度问题
                this.handleDataType(sqlBuilder, columnInfo);
            }
            sqlBuilder.append(" ");
            if (columnInfo.isNotNull()) {
                sqlBuilder.append("NOT NULL ");
            }


            // 字符集 & 排序规则
            if (this.isSameDatabaseVendor()
                    && StringUtils.isNotEmpty(columnInfo.getCollationName())
                    && !ti.getTableCollation().equals(columnInfo.getCollationName())) {
                // 数据库厂家相同
                // 且不同于默认表的默认排序规则&字符集
                sqlBuilder.append("CHARACTER SET ").append(columnInfo.getCharsetName()).append(" ");
                sqlBuilder.append("COLLATE ").append(columnInfo.getCollationName()).append(" ");
            } else {
                // 数据库厂家不同
            }

            // 默认值
            if (columnInfo.getColumnDef() != null) {
                // 时间默认值
                String dataTypeDef = this.getDatabaseDataTypeDefault(columnInfo.getColumnDef());
                sqlBuilder.append("DEFAULT ").append(dataTypeDef == null ? columnInfo.getColumnDef(): dataTypeDef).append(" ");
            }

            // 其他信息
            if (this.isSameDatabaseVendor()
                    && StringUtils.isNotEmpty(columnInfo.getExtra())
                    && ! columnInfo.getExtra().equalsIgnoreCase(this.getAutoincrement())) {
                sqlBuilder.append(columnInfo.getExtra()).append(" ");
            }

            // 自动增长
            if(columnInfo.isAutoincrement()){
                // MySQL不支持非主键且设置自动增长的列，
                // 否则会抛出：Incorrect table definition; there can be only one auto column and it must be defined as a key
                // 因此如果表信息中没有主键信息的话，默认设置
                if(ti.getPrimaryKeys().size() == 0
                        && this.getDatabaseConfig().isPrimaryKeyWithAutoincrementColumns()){
                    sqlBuilder.append(" PRIMARY KEY ");
                }
                sqlBuilder.append(this.getAutoincrement()).append(" ");

            }

            if (StringUtils.isNotEmpty(columnInfo.getRemarks())) {
                sqlBuilder.append("COMMENT '").append(columnInfo.getRemarks()).append("' ");
            }

            sqlBuilder.append(",");

        });

        // 主键
        buildPrimaryKeys(ti, sqlBuilder);

        // 约束
        buildConstraintInfo(ti, sqlBuilder);

        sqlBuilder.delete(sqlBuilder.length() - 1, sqlBuilder.length()).append(") ");
        if (ti.getAutoIncrement() != null) {
            sqlBuilder.append("AUTO_INCREMENT=").append(ti.getAutoIncrement()).append(" ");
        }

        if (isSameDatabaseVendor() && ti.getTableCollation() != null) {
            sqlBuilder.append("DEFAULT COLLATE=").append(ti.getTableCollation()).append(" ");
        }

        if (StringUtils.isNotEmpty(ti.getRemarks())) {
            sqlBuilder.append("COMMENT='").append(ti.getRemarks()).append("' ");
        }

        return sqlBuilder;
    }

    /**
     * 处理字符串、数字、日期的长度、精度问题
     *
     * @param sqlBuilder StringBuilder
     * @param columnInfo {@link ColumnInfo}
     */
    @Override
    protected void handleDataType(StringBuilder sqlBuilder, ColumnInfo columnInfo) {
        MysqlType mysqlType = MysqlType.getByJdbcType(columnInfo.getDataType());
        switch (mysqlType) {
            case ENUM:
            case SET:
                // 其他数据库是否支持ENUM，SET？
                break;

            case FLOAT:
            case FLOAT_UNSIGNED:
            case DECIMAL:
            case DECIMAL_UNSIGNED:
            case DOUBLE:
            case DOUBLE_UNSIGNED:
                // ERROR 1427 (42000): For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column 'a')
                // 由于float和double存在精度丢失的问题，因此转换过来的类型均使用DECIMAL.
                // columnInfo.getColumnSize() == 0 的时候，默认保存到数据库为10
                if(columnInfo.getColumnSize() <= 0){
                    columnInfo.setColumnSize(10);
                }
                sqlBuilder.append(MysqlType.DECIMAL.getName()).append("(").append(columnInfo.getColumnSize());
                if (columnInfo.getDecimalDigits() > 0) {
                    // columnInfo.getDecimalDigits() < 0 为语法错误
                    sqlBuilder.append(",").append(Math.min(columnInfo.getDecimalDigits(), columnInfo.getColumnSize()));

                }
                sqlBuilder.append(")");

                break;

            case CHAR:
                if(columnInfo.getColumnSize() > MysqlType.CHAR.getPrecision()){
                    log.info("列{}字段类型{}长度大于{}，已转换为{}({})",
                            columnInfo.getColumnName(), mysqlType.getName(), columnInfo.getColumnSize(), MysqlType.VARCHAR.getName(), columnInfo.getColumnSize());
                    mysqlType = MysqlType.VARCHAR;
                }
            case VARCHAR:
                if (columnInfo.getColumnSize() > MysqlType.MEDIUMTEXT.getPrecision()) {
                    sqlBuilder.append(MysqlType.LONGTEXT.getName());
                } else if (columnInfo.getColumnSize() > MysqlType.VARCHAR.getPrecision()) {
                    sqlBuilder.append(MysqlType.MEDIUMTEXT.getName());
                } else {
                    sqlBuilder.append(mysqlType.getName()).append("(").append(columnInfo.getColumnSize()).append(")");
                }
                break;
            case BINARY:
            case VARBINARY:
                if (columnInfo.getColumnSize() > MysqlType.MEDIUMBLOB.getPrecision()) {
                    sqlBuilder.append(MysqlType.LONGBLOB.getName());
                } else if (columnInfo.getColumnSize() > MysqlType.VARBINARY.getPrecision()) {
                    sqlBuilder.append(MysqlType.MEDIUMBLOB.getName());
                } else {
                    sqlBuilder.append(mysqlType.getName()).append("(").append(columnInfo.getColumnSize()).append(")");
                }
                break;

            case DATE:
            case TIME:
            case DATETIME:
            case TIMESTAMP:
                if(columnInfo.getTypeName().toUpperCase().contains("DATETIME")){
                    sqlBuilder.append(MysqlType.DATETIME.getName());
                    break;
                }

            case TINYTEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case JSON:
            case TEXT:
            case TINYBLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
            case BLOB:
            case BIT:

            case TINYINT:
            case TINYINT_UNSIGNED:

            case BOOLEAN:
            case GEOMETRY:
            case NULL:
            case UNKNOWN:
            case YEAR:

            default:
                sqlBuilder.append(mysqlType.getName());
        }

    }

    @Override
    public String getRightName(String name) {
        if(this.getDatabaseConfig().isLowerCaseName()){
            name = name.toLowerCase();
        }
        return String.format("`%s`", name);
    }

    // @Override
    // public Map<DateTypeEnum, String[]> getDateTypeDefaults() {
    //     return new HashMap<DateTypeEnum, String[]>() {
    //         {
    //             // select NOW(),CURRENT_TIMESTAMP(),SYSDATE(),SLEEP(2),NOW(),CURRENT_TIMESTAMP(),SYSDATE();
    //             // 实际上，NOW和CURRENT_TIMESTAMP没有任何区别，他们都表示的是SQL开始执行时的系统时间；
    //             // 而SYSDATE则表示执行此函数时的系统时间。
    //
    //             // https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html
    //             // https://mariadb.com/kb/en/current_time/
    //             put(DateTypeEnum.DATE, new String[]{"curdate()", "current_date()", "current_date"});
    //             put(DateTypeEnum.DATETIME, new String[]{"sysdate()"});
    //             put(DateTypeEnum.TIME, new String[]{"curtime()", "current_time()", "current_time", "localtime()", "localtime"});
    //             put(DateTypeEnum.TIMESTAMP, new String[]{"now()", "current_timestamp", "current_timestamp()", "localtimestamp", "localtimestamp()"});
    //         }
    //     };
    // }

    @Override
    public List<String> getUnsupportedFunctions() {
        return new ArrayList<String>(){
            {
                add("CONV()");
                add("CRC32()");
                add("DIV");
                add("LOG2()");

                add("ADDDATE()");
                add("ADDTIME()");
                add("CONVERT_TZ()");
                add("DATE()");
                add("DATE_ADD()");
                add("DATE_FORMAT()");
                add("DATE_SUB()");
                add("DAY()");
                add("DAYNAME()");
                add("DAYOFWEEK()");
                add("DAYOFYEAR()");
                add("FROM_DAYS()");
                add("GET_FORMAT()");
                add("LAST_DAY()");
                add("MICROSECOND()");
                add("MINUTE()");
                add("MONTH()");
                add("MONTHNAME()");
                add("PERIOD_ADD()");
                add("PERIOD_DIFF()");
                add("QUARTER()");
                add("SEC_TO_TIME()");
                add("SUBTIME()");
                add("TIME()");
                add("TIME_TO_SEC()");
                add("DATEDIFF()");
                add("TIMEDIFF()");
                add("TIMESTAMPADD()");
                add("TO_DAYS()");
                add("TO_SECONDS()");
                add("UTC_DATE()");
                add("UTC_TIME()");
                add("UTC_TIMESTAMP()");
                add("WEEK()");
                add("WEEKDAY()");
                add("WEEKOFYEAR()");
                add("YEAR()");
                add("YEARWEEK()");

                add("BIN()");
                add("ELT()");
                add("EXPORT_SET()");
                add("FIND_IN_SET()");
                add("FORMAT()");
                add("FROM_BASE64()");
                add("INSERT()");
                add("LIKE");
                add("LOAD_FILE()");
                add("LOCATE()");
                add("MAKE_SET()");
                add("MATCH");
                add("");
                add("NOT");
                add("NOT");
                add("OCT()");
                add("ORD()");
                add("QUOTE()");
                add("REGEXP");
                add("REGEXP_INSTR()");
                add("REGEXP_LIKE()");
                add("REGEXP_REPLACE()");
                add("REGEXP_SUBSTR()");
                add("RLIKE");
                add("SOUNDEX()");
                add("SOUNDS");
                add("SPACE()");
                add("STRCMP()");
                add("SUBSTRING_INDEX()");
                add("TO_BASE64()");
                add("UNHEX()");
                add("WEIGHT_STRING()");
            }
        };
    }

    @Override
    public void applySlaveDatabaseMetadata() {
        // 应用元数据信息
    }

    @Override
    public void buildDatabaseFunctionMap(Map<DatabaseFunctionEnum, String[]> map) {

        // 数学函数
        // https://dev.mysql.com/doc/refman/8.0/en/numeric-functions.html
        // 不支持：
        // CONV()
        // CRC32()
        // DIV
        // LOG2()
        map.put(MathematicalFunctionEnum.abs, new String[]{"ABS()"});
        // map.put(MathematicalFunctionEnum.cbrt, new String[]{""});
        map.put(MathematicalFunctionEnum.ceil, new String[]{"CEIL()"});
        map.put(MathematicalFunctionEnum.ceiling, new String[]{"CEILING()"});
        map.put(MathematicalFunctionEnum.degrees, new String[]{"DEGREES()"});
        // map.put(MathematicalFunctionEnum.div, new String[]{"DIV"});
        map.put(MathematicalFunctionEnum.exp, new String[]{"EXP()"});
        // map.put(MathematicalFunctionEnum.factorial, new String[]{""});
        map.put(MathematicalFunctionEnum.floor, new String[]{"FLOOR()"});
        // map.put(MathematicalFunctionEnum.gcd, new String[]{""});
        // map.put(MathematicalFunctionEnum.lcm, new String[]{""});
        map.put(MathematicalFunctionEnum.ln, new String[]{"LN()"});
        map.put(MathematicalFunctionEnum.log, new String[]{"LOG()"});
        map.put(MathematicalFunctionEnum.log10, new String[]{"LOG10()"});
        // map.put(MathematicalFunctionEnum.min_scale, new String[]{""});
        map.put(MathematicalFunctionEnum.mod, new String[]{"MOD()"});
        map.put(MathematicalFunctionEnum.pi, new String[]{"PI()"});
        map.put(MathematicalFunctionEnum.power, new String[]{"POW()", "POWER()"});
        map.put(MathematicalFunctionEnum.radians, new String[]{"RADIANS()"});
        map.put(MathematicalFunctionEnum.round, new String[]{"ROUND()"});
        // map.put(MathematicalFunctionEnum.scale, new String[]{""});
        map.put(MathematicalFunctionEnum.sign, new String[]{"SIGN()"});
        map.put(MathematicalFunctionEnum.sqrt, new String[]{"SQRT()"});
        // map.put(MathematicalFunctionEnum.trim_scale, new String[]{""});
        map.put(MathematicalFunctionEnum.trunc, new String[]{"TRUNCATE()"});
        // map.put(MathematicalFunctionEnum.width_bucket, new String[]{""});
        map.put(MathematicalFunctionEnum.random, new String[]{"RAND()"});
        map.put(MathematicalFunctionEnum.acos, new String[]{"ACOS()"});
        // map.put(MathematicalFunctionEnum.acosd, new String[]{""});
        map.put(MathematicalFunctionEnum.asin, new String[]{"ASIN()"});
        // map.put(MathematicalFunctionEnum.asind, new String[]{""});
        map.put(MathematicalFunctionEnum.atan, new String[]{"ATAN()"});
        // map.put(MathematicalFunctionEnum.atand, new String[]{""});
        map.put(MathematicalFunctionEnum.atan2, new String[]{"ATAN2()"});
        // map.put(MathematicalFunctionEnum.atan2d, new String[]{""});
        map.put(MathematicalFunctionEnum.cos, new String[]{"COS()"});
        // map.put(MathematicalFunctionEnum.cosd, new String[]{""});
        map.put(MathematicalFunctionEnum.cot, new String[]{"COT()"});
        // map.put(MathematicalFunctionEnum.cotd, new String[]{""});
        map.put(MathematicalFunctionEnum.sin, new String[]{"SIN()"});
        // map.put(MathematicalFunctionEnum.sind, new String[]{""});
        map.put(MathematicalFunctionEnum.tan, new String[]{"TAN()"});
        // map.put(MathematicalFunctionEnum.tand, new String[]{""});


        // 日期
        // https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html
        // 不支持
        // ADDDATE()        DATE_ADD('1998-01-02', INTERVAL 31 DAY);
        // ADDTIME()        ADDTIME('1997-12-31 23:59:59.999999','1 1:1:1.000002');
        // CONVERT_TZ()     CONVERT_TZ('2004-01-01 12:00:00','+00:00','+10:00')
        // DATE()           字符串转时间
        // DATE_ADD()
        // DATE_FORMAT()
        // DATE_SUB() = SUBDATE()
        // DAY() = DAYOFMONTH()
        // DAYNAME()
        // DAYOFWEEK()
        // DAYOFYEAR()
        // FROM_DAYS()
        // GET_FORMAT()
        // LAST_DAY()
        // MICROSECOND()
        // MINUTE()
        // MONTH()
        // MONTHNAME()
        // PERIOD_ADD()
        // PERIOD_DIFF()
        // QUARTER()
        // SEC_TO_TIME()
        // SUBTIME()
        // TIME()
        // TIME_TO_SEC()
        // DATEDIFF()
        // TIMEDIFF()
        // TIMESTAMPADD()
        // TO_DAYS()
        // TO_SECONDS()
        // UTC_DATE()
        // UTC_TIME()
        // UTC_TIMESTAMP()
        // WEEK()
        // WEEKDAY()
        // WEEKOFYEAR()
        // YEAR()
        // YEARWEEK()

        map.put(DateTypeFunctionEnum.to_char, new String[]{"TIME_FORMAT()"});
        map.put(DateTypeFunctionEnum.to_date, new String[]{"STR_TO_DATE()"});
        // map.put(DateTypeFunctionEnum.to_number, new String[]{""});                      //
        map.put(DateTypeFunctionEnum.to_timestamp, new String[]{"FROM_UNIXTIME()"});    // 数字转timestamp

        map.put(DateTypeFunctionEnum.age, new String[]{"TIMESTAMPDIFF()"});
        map.put(DateTypeFunctionEnum.clock_timestamp, new String[]{"SYSDATE()"});   // Current date and time (changes during statement execution);
        map.put(DateTypeFunctionEnum.current_date, new String[]{"CURDATE()", "CURRENT_DATE()", "CURRENT_DATE"});
        map.put(DateTypeFunctionEnum.current_time, new String[]{"CURTIME()", "CURRENT_TIME()", "CURRENT_TIME"});
        map.put(DateTypeFunctionEnum.current_timestamp, new String[]{"CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP"});
        // map.put(DateTypeFunctionEnum.date_part, new String[]{""});
        // map.put(DateTypeFunctionEnum.date_trunc, new String[]{""});
        map.put(DateTypeFunctionEnum.extract, new String[]{"EXTRACT"});
        // map.put(DateTypeFunctionEnum.isfinite, new String[]{""});
        // map.put(DateTypeFunctionEnum.justify_days, new String[]{""});
        // map.put(DateTypeFunctionEnum.justify_hours, new String[]{""});
        // map.put(DateTypeFunctionEnum.justify_interval, new String[]{""});
        map.put(DateTypeFunctionEnum.localtime, new String[]{"LOCALTIME()", "LOCALTIME"});                  // Synonym for NOW()
        map.put(DateTypeFunctionEnum.localtimestamp, new String[]{"LOCALTIMESTAMP()", "LOCALTIMESTAMP"});   // Synonym for NOW()
        map.put(DateTypeFunctionEnum.make_date, new String[]{"MAKEDATE()"});    // 存在差异。。。
        // map.put(DateTypeFunctionEnum.make_interval, new String[]{""});          //
        map.put(DateTypeFunctionEnum.make_time, new String[]{"MAKETIME()"});
        map.put(DateTypeFunctionEnum.make_timestamp, new String[]{"TIMESTAMP()"});
        map.put(DateTypeFunctionEnum.make_timestamptz, new String[]{"TIMESTAMP()"});

        map.put(DateTypeFunctionEnum.now, new String[]{"NOW()"});    // Current date and time (start of current transaction);
        map.put(DateTypeFunctionEnum.statement_timestamp, new String[]{"SYSDATE()"});   // Current date and time (start of current statement);
        map.put(DateTypeFunctionEnum.timeofday, new String[]{"UTC_TIME()"});   //
        map.put(DateTypeFunctionEnum.transaction_timestamp, new String[]{"SYSDATE()"});  // Current date and time (start of current transaction);


        // 字符串
        // https://dev.mysql.com/doc/refman/8.0/en/string-functions.html
        // 不支持
        // BIN()
        // ELT() = FIELD() -> FIND_IN_SET()
        // EXPORT_SET()
        // FIND_IN_SET()
        // FORMAT()
        // FROM_BASE64()
        // INSERT()
        // LIKE
        // LOAD_FILE()
        // LOCATE()     // INSTR替换
        // MAKE_SET()
        // MATCH
        //
        // NOT LIKE
        // NOT REGEXP
        // OCT()
        // ORD()
        // QUOTE()
        // REGEXP
        // REGEXP_INSTR()
        // REGEXP_LIKE()
        // REGEXP_REPLACE()
        // REGEXP_SUBSTR()
        // RLIKE
        // SOUNDEX()
        // SOUNDS LIKE
        // SPACE()
        // STRCMP()
        // SUBSTRING_INDEX()
        // TO_BASE64()  ----> encode('123', 'base64');
        // UNHEX()      -- from Hex
        // WEIGHT_STRING()

        map.put(StringFunctionEnum.bit_length, new String[]{"BIT_LENGTH()"});
        map.put(StringFunctionEnum.char_length, new String[]{"CHAR_LENGTH()"});
        map.put(StringFunctionEnum.character_length, new String[]{"CHARACTER_LENGTH()"});
        map.put(StringFunctionEnum.lower, new String[]{"LCASE()", "LOWER()"});
        // map.put(StringFunctionEnum.normalize, new String[]{""});
        map.put(StringFunctionEnum.octet_length, new String[]{"OCTET_LENGTH()"});
        // map.put(StringFunctionEnum.overlay, new String[]{""});
        map.put(StringFunctionEnum.position, new String[]{"POSITION()"});
        map.put(StringFunctionEnum.substring, new String[]{"SUBSTRING()", "MID()"});    // MID(str,pos,len) is a synonym for SUBSTRING(str,pos,len).
        map.put(StringFunctionEnum.trim, new String[]{"TRIM()"});
        map.put(StringFunctionEnum.upper, new String[]{"UCASE()", "UPPER()"});
        map.put(StringFunctionEnum.ascii, new String[]{"ASCII()"});
        // map.put(StringFunctionEnum.btrim, new String[]{""});
        map.put(StringFunctionEnum.chr, new String[]{"CHAR()"});
        map.put(StringFunctionEnum.concat, new String[]{"CONCAT()"});
        map.put(StringFunctionEnum.concat_ws, new String[]{"CONCAT_WS()"});
        // map.put(StringFunctionEnum.format, new String[]{""});   // FORMAT() 差异化...
        // map.put(StringFunctionEnum.initcap, new String[]{""});
        map.put(StringFunctionEnum.left, new String[]{"LEFT()"});
        map.put(StringFunctionEnum.length, new String[]{"LENGTH()"});
        map.put(StringFunctionEnum.lpad, new String[]{"LPAD()"});
        map.put(StringFunctionEnum.ltrim, new String[]{"LTRIM()"});
        map.put(StringFunctionEnum.md5, new String[]{"MD5()"});
        // 正则暂时不处理
        // map.put(StringFunctionEnum.parse_ident, new String[]{""});
        // map.put(StringFunctionEnum.quote_ident, new String[]{""});
        // map.put(StringFunctionEnum.quote_literal, new String[]{""});
        // map.put(StringFunctionEnum.quote_nullable, new String[]{""});
        // map.put(StringFunctionEnum.regexp_match, new String[]{""});
        // map.put(StringFunctionEnum.regexp_matches, new String[]{""});
        // map.put(StringFunctionEnum.regexp_replace, new String[]{""});
        // map.put(StringFunctionEnum.regexp_split_to_array, new String[]{""});
        // map.put(StringFunctionEnum.regexp_split_to_table, new String[]{""});
        // 正则暂时不处理 end...
        map.put(StringFunctionEnum.repeat, new String[]{"REPEAT()"});
        map.put(StringFunctionEnum.replace, new String[]{"REPLACE()"});
        map.put(StringFunctionEnum.reverse, new String[]{"REVERSE()"});
        map.put(StringFunctionEnum.right, new String[]{"RIGHT()"});
        map.put(StringFunctionEnum.rpad, new String[]{"RPAD()"});
        map.put(StringFunctionEnum.rtrim, new String[]{"RTRIM()"});
        // map.put(StringFunctionEnum.split_part, new String[]{""});
        map.put(StringFunctionEnum.strpos, new String[]{"INSTR()"});
        map.put(StringFunctionEnum.substr, new String[]{"SUBSTR()"});
        // map.put(StringFunctionEnum.starts_with, new String[]{""});
        // map.put(StringFunctionEnum.to_ascii, new String[]{""});
        map.put(StringFunctionEnum.to_hex, new String[]{"HEX()"});
        // map.put(StringFunctionEnum.translate, new String[]{""});

        // bytes
        // map.put(StringFunctionEnum.convert, new String[]{""});
        // map.put(StringFunctionEnum.convert_from, new String[]{""});
        // map.put(StringFunctionEnum.convert_to, new String[]{""});
        // map.put(StringFunctionEnum.encode, new String[]{""});
        // map.put(StringFunctionEnum.decode, new String[]{""});
        map.put(StringFunctionEnum.uuid, new String[]{"UUID()"});

    }

    /**
     * 构造索引
     * 参考：https://dev.mysql.com/doc/refman/5.6/en/alter-table.html
     *
     * @param ti 表信息
     */
    @Override
    public void buildIndex(TableInfo ti) {
        if (ti == null || ti.getIndexInfo() == null) {
            return;
        }

        // 一个键名，多个列
        StringBuilder sBuilder = null;
        List<String> buildIndex = null;
        String indexType;

        for (IndexInfo2 ii : ti.getIndexInfo()) {

            if(ii.isTableIndexStatistic()){
                // 统计信息不需要。
                continue;
            }

            if(sBuilder == null){
                sBuilder = new StringBuilder();
            }
            if(buildIndex == null){
                buildIndex = new ArrayList<>();
            }

            // MySQL所有的索引默认是 DatabaseMetaData.tableIndexClustered
            if (buildIndex.contains(ii.getIndexName())) {
                continue;
            }
            buildIndex.add(ii.getIndexName());

            // alter table tName add
            sBuilder.append("ALTER TABLE ").append(this.getRightName(ti.getTableName())).append(" ADD ");

            // ADD INDEX [INDEX_NAME] [index_type] (key_part,...)
            // ADD FULLTEXT/SPATIAL INDEX [index_name] (key_part,...)
            // ------ ADD CONSTRAINT [symbol] PRIMARY KEY [index_type] (key_part,...)
            // ADD CONSTRAINT [symbol] UNIQUE [INDEX | KEY] [index_name] [index_type] (key_part,...)
            // ----- ADD CONSTRAINT [symbol] FOREIGN KEY [index_name] (col_name,...)
            if (!ii.isNonUnique()) {
                indexType = SlaveMetaDataEntity.TYPE_UNIQUE_INDEX;
                sBuilder.append("CONSTRAINT UNIQUE ");
            } else if(this.isSameDatabaseVendor() &&
                    (IndexTypeEnum.FULLTEXT.toString().equalsIgnoreCase(ii.getIndexTypeDesc())
                            || IndexTypeEnum.SPATIAL.toString().equalsIgnoreCase(ii.getIndexTypeDesc()))){
                // 全文索引，空间索引
                sBuilder.append("ADD ").append(ii.getIndexTypeDesc()).append(" INDEX ");

                indexType = SlaveMetaDataEntity.TYPE_INDEX;
            } else {
                // 普通索引
                sBuilder.append("INDEX ");
                indexType = SlaveMetaDataEntity.TYPE_INDEX;
            }

            // [index_type]
            if(ii.getIndexTypeDesc() != null){
                switch (ii.getIndexTypeDesc()){
                    case "BTREE":
                    case "HASH":
                        sBuilder.append("USING ").append(ii.getIndexTypeDesc());
                        break;
                    default:
                }
            }

            // [index_name]
            String indexName = ii.getIndexName();
            if(this.getDatabaseConfig().isGenerateName()){
                // 自动生成
                indexName = this.buildIndexName(ii, ti);
            }

            sBuilder.append(this.getRightName(indexName)).append(" ");
            ti.getIndexNames().add(ii.getIndexName());

            // 索引类型
            // https://dev.mysql.com/doc/refman/8.0/en/information-schema-statistics-table.html
            //     // INDEX_TYPE:
            //     // The index method used (BTREE, FULLTEXT, HASH, RTREE)

            // column-list
            sBuilder.append("(");

            // 获取所有的columnName
            for (IndexInfo2 ii2 : ti.getIndexInfo()) {
                if (!ii.getIndexName().equals(ii2.getIndexName())) {
                    continue;
                }
                sBuilder.append(this.getRightName(ii2.getColumnName())).append(" ").append(ii2.getOrder()).append(",");
            }

            sBuilder.delete(sBuilder.length() - 1, sBuilder.length()).append(")");

            this.databaseDao.insertMetadata(new SlaveMetaDataEntity(ti.getTableName(), indexType, sBuilder.toString()));

            // 全部清掉
            sBuilder.delete(0, sBuilder.length());
        }

    }


    /**
     * 索引名称
     * @param ii 索引
     * @param ti 表信息
     * @return 索引名称
     */
    private String buildIndexName(IndexInfo2 ii, TableInfo ti){

        StringBuilder indexNameBuilder = new StringBuilder();
        final int indexNameMaxLength = 64;

        if(ii.isNonUnique()){
            indexNameBuilder.append("idx_");
        } else {
            indexNameBuilder.append("ux_");
        }
        indexNameBuilder.append(ti.getTableName().toLowerCase()).append("_");

        StringBuilder columnBuilder = new StringBuilder();
        // 获取所有的columnName
        for (IndexInfo2 ii2 : ti.getIndexInfo()) {
            if (!ii.getIndexName().equals(ii2.getIndexName())) {
                continue;
            }
            columnBuilder.append(ii2.getColumnName().toLowerCase()).append("_");
        }
        columnBuilder.delete(columnBuilder.length() - 1, columnBuilder.length());

        if(ti.getIndexNames() == null){
            ti.setIndexNames(new ArrayList<>());
        }

        // 最长64个字符、
        if((indexNameBuilder.length() + columnBuilder.length()) > indexNameMaxLength){
            // 组装所有列的时候，超过长度限制。
            int len = indexNameMaxLength - indexNameBuilder.length();
            int i = 1;
            do {
                if (len > 10) {
                    indexNameBuilder.append("comb_cols_").append(i);
                } else if (len >= 6) {
                    indexNameBuilder.append("cols_").append(i);
                } else if (len > 1) {
                    indexNameBuilder.append("_").append(i);
                } else {
                    break;
                }
                i++;
            } while (ti.getIndexNames().contains(indexNameBuilder.toString()));

        } else {
            indexNameBuilder.append(columnBuilder);
        }

        ti.getIndexNames().add(indexNameBuilder.toString());
        return indexNameBuilder.toString();
    }

    @Override
    protected void buildForeignKeyChangeRules(StringBuilder sBuilder, ImportedKey importedKey) {
        // 删除规则
        // As of NDB 8.0.16: For NDB tables, ON DELETE CASCADE is not supported where the child table contains one or more columns of any of the TEXT or BLOB types. (Bug #89511, Bug #27484882)
        // https://dev.mysql.com/doc/refman/8.0/en/create-table-foreign-keys.html
        // For an ON DELETE or ON UPDATE that is not specified, the default action is always NO ACTION.
        if(importedKey.getDeleteRule() == ImportedKey.SET_DEFAULT){
            importedKey.setDeleteRule(ImportedKey.NO_ACTION);
        }
        sBuilder.append(" ").append(importedKey.getDeleteAction()).append(" ");

        // 更新规则
        // For an ON DELETE or ON UPDATE that is not specified, the default action is always NO ACTION.
        if(importedKey.getUpdateRule() == ImportedKey.SET_DEFAULT){
            importedKey.setUpdateRule(ImportedKey.NO_ACTION);
        }
        sBuilder.append(" ").append(importedKey.getUpdateAction()).append(" ");
    }

    @Override
    public BasicDatabaseDao getDatabaseDao() {
        return this.databaseDao;
    }

    @Override
    public String getPaginationSQL(TableInfo ti, int offset, int limit) {
        // 获取主键
        if(ti.getPrimaryKeys().size() == 1){
            // 获取主键列名
            final String colName = ti.getPrimaryKeys().get(0).getColumnName();
            // 延迟关联
            // https://www.cnblogs.com/wang-meng/p/ae6d1c4a7b553e9a5c8f46b67fb3e3aa.html
            return String.format("SELECT * FROM %s INNER JOIN (SELECT %s FROM %s ORDER BY %s LIMIT %s, %s) AS X USING(%s)",
                    ti.getTableName(), colName, ti.getTableName(), colName, offset, limit, colName);
        }

        // 无主键、多主键使用默认的分页查询方式
        return String.format("SELECT * FROM %s LIMIT %s, %s", ti.getTableName(), offset, limit);
    }


    @Override
    public String getDatabaseVendor() {
        return DatabaseVendorEnum.MYSQL.getVendor();
    }

    @Override
    public String getAutoincrement() {
        return "AUTO_INCREMENT";
    }
}
