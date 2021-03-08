package com.liaobaikai.ngoxdb.core.service.impl.converter;

import com.liaobaikai.ngoxdb.bean.info.ColumnInfo;
import com.liaobaikai.ngoxdb.bean.info.IndexInfo2;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.bean.rs.ImportedKey;
import com.liaobaikai.ngoxdb.bean.rs.PrimaryKey;
import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
import com.liaobaikai.ngoxdb.core.config.DatabaseConfig;
import com.liaobaikai.ngoxdb.core.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.core.dao.impl.MySQLDatabaseDao;
import com.liaobaikai.ngoxdb.core.entity.SlaveMetaDataEntity;
import com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum;
import com.liaobaikai.ngoxdb.core.enums.IndexTypeEnum;
import com.liaobaikai.ngoxdb.core.enums.fn.DatabaseFunctionEnum;
import com.liaobaikai.ngoxdb.core.enums.fn.DateTypeFunctionEnum;
import com.liaobaikai.ngoxdb.core.enums.fn.MathematicalFunctionEnum;
import com.liaobaikai.ngoxdb.core.enums.fn.StringFunctionEnum;
import com.liaobaikai.ngoxdb.core.service.DatabaseComparator;
import com.liaobaikai.ngoxdb.core.service.impl.comparator.MySQLDatabaseComparator;
import com.mysql.cj.MysqlType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

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

    public MySQLDatabaseConverter(JdbcTemplate2 jdbcTemplate,
                                  boolean isMaster,
                                  String masterDatabaseVendor,
                                  DatabaseConfig databaseConfig) {
        super(jdbcTemplate, isMaster, masterDatabaseVendor, databaseConfig);
        this.databaseDao = new MySQLDatabaseDao(jdbcTemplate);
        this.databaseComparator = new MySQLDatabaseComparator(this);
        this.truncateMetadata();
    }

    @Override
    public Logger getLogger() {
        return log;
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
    public String buildCreateTable(TableInfo ti) {

        StringBuilder sqlBuilder = new StringBuilder("CREATE TABLE ");
        sqlBuilder.append(this.getRightName(ti.getTableName())).append(" (");

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
                String dataTypeDef = this.getDatabaseDataTypeDefault(columnInfo.getColumnDef(), columnInfo.getDataType());
                sqlBuilder.append("DEFAULT ").append(dataTypeDef == null ? columnInfo.getColumnDef() : dataTypeDef).append(" ");
            }

            // 其他信息
            if (this.isSameDatabaseVendor()
                    && StringUtils.isNotEmpty(columnInfo.getExtra())
                    && !columnInfo.getExtra().equalsIgnoreCase(this.getAutoincrement())) {
                sqlBuilder.append(columnInfo.getExtra()).append(" ");
            }

            // 自动增长
            if (columnInfo.isAutoincrement()) {
                // MySQL不支持非主键且设置自动增长的列，
                // 否则会抛出：Incorrect table definition; there can be only one auto column and it must be defined as a key
                // 因此如果表信息中没有主键信息的话，默认设置
                if (ti.getPrimaryKeys().size() == 0
                        && this.getDatabaseConfig().isPrimaryKeyWithAutoincrementColumns()) {
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

        return sqlBuilder.toString();
    }

    /**
     * 处理字符串、数字、日期的长度、精度问题
     *
     * @param sqlBuilder StringBuilder
     * @param columnInfo {@link ColumnInfo}
     */
    @Override
    protected void handleDataType(StringBuilder sqlBuilder, ColumnInfo columnInfo) {

        String typeName = "";   // 类型名称
        int length = -1;        // 字符长度
        int precision = -1;     // 精度
        int scale = -1;         // 刻度

        int chars = columnInfo.getColumnSize();
        int bytes = columnInfo.getCharOctetLength();

        switch (columnInfo.getDataType()) {
            case Types.BIT:
                typeName = MysqlType.BIT.getName();
                length = Math.min(columnInfo.getColumnSize(), 64);
                break;
            case Types.TINYINT:
                typeName = MysqlType.TINYINT.getName();
                break;
            case Types.SMALLINT:
                typeName = MysqlType.SMALLINT.getName();
                break;
            case Types.INTEGER:
                typeName = MysqlType.INT.getName();
                break;
            case Types.BIGINT:
                typeName = MysqlType.BIGINT.getName();
                break;
            case Types.FLOAT:
                typeName = MysqlType.FLOAT.getName();
                break;
            case Types.REAL:
                typeName = "real";
                break;
            case Types.DOUBLE:
                typeName = MysqlType.DOUBLE.getName();
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                typeName = MysqlType.DECIMAL.getName();
                // ERROR 1427 (42000): For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column 'a')
                // 由于float和double存在精度丢失的问题，因此转换过来的类型均使用DECIMAL.
                // columnInfo.getColumnSize() == 0 的时候，默认保存到数据库为10
                precision = Math.min(columnInfo.getColumnSize(), MysqlType.DECIMAL.getPrecision().intValue());

                // 刻度
                if (columnInfo.getDecimalDigits() > 0) {
                    // columnInfo.getDecimalDigits() < 0 为语法错误
                    scale = Math.min(columnInfo.getDecimalDigits(), columnInfo.getColumnSize());
                } else {

                    if (precision == 3) {
                        typeName = "tinyint";
                        precision = -1;
                    } else if (precision == 5) {
                        typeName = "smallint";
                        precision = -1;
                    } else if (precision == 10 || precision == 0) {
                        typeName = "integer";
                        precision = -1;
                    } else if (precision == 19) {
                        typeName = "bigint";
                        precision = -1;
                    }
                }

                break;
            case Types.NCHAR:
            case Types.CHAR:
                if (chars > MysqlType.CHAR.getPrecision()) {
                    // max:65535L
                    typeName = MysqlType.VARCHAR.getName();
                } else {
                    // max:255L
                    typeName = MysqlType.CHAR.getName();
                }
                length = chars;
                break;
            case Types.NVARCHAR:
            case Types.VARCHAR:
                if (chars > MysqlType.MEDIUMTEXT.getPrecision()) {
                    // max:4294967295L
                    typeName = MysqlType.LONGTEXT.getName();
                    break;
                } else if (chars > MysqlType.VARCHAR.getPrecision()) {
                    // max:16777215L
                    typeName = MysqlType.MEDIUMTEXT.getName();
                    break;
                }
                // max:65535L
                typeName = MysqlType.VARCHAR.getName();
                length = chars;
                break;
            case Types.LONGNVARCHAR:
            case Types.NCLOB:
            case Types.CLOB:
            case Types.LONGVARCHAR:
                typeName = MysqlType.LONGTEXT.getName();
                break;
            case Types.DATE:
                typeName = MysqlType.DATE.getName();
                break;
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                typeName = MysqlType.TIME.getName();
                break;
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                // datetime: 支持 1000-01-01 00:00:00 到 9999-12-31 23:59:59
                // timestamp: 支持 1970-01-01 00:00:01 UTC to 2038-01-19 03:14:07
                typeName = MysqlType.TIMESTAMP.getName();
                break;
            case Types.BINARY:
                if (bytes > MysqlType.MEDIUMBLOB.getPrecision()) {
                    // max:4294967295L
                    typeName = MysqlType.LONGBLOB.getName();
                    break;
                } else if (bytes > MysqlType.BLOB.getPrecision() - 3) {
                    // -3: len: 2, null: 1
                    // max:65535L
                    typeName = MysqlType.MEDIUMBLOB.getName();
                    break;
                } else if (bytes > MysqlType.BINARY.getPrecision()) {
                    // max:65535L
                    typeName = MysqlType.VARBINARY.getName();
                } else {
                    // max:255L
                    typeName = MysqlType.BINARY.getName();
                }
                length = bytes;
                break;
            case Types.VARBINARY:
                if (bytes > MysqlType.MEDIUMBLOB.getPrecision()) {
                    // max:4294967295L
                    typeName = MysqlType.LONGBLOB.getName();
                    break;
                } else if (bytes > MysqlType.BLOB.getPrecision() - 3) {
                    // -3: len: 2, null: 1
                    // max:65535L
                    typeName = MysqlType.MEDIUMBLOB.getName();
                    break;
                }
                typeName = MysqlType.VARBINARY.getName();
                length = bytes;
                break;
            case Types.BLOB:
            case Types.LONGVARBINARY:
                typeName = MysqlType.LONGBLOB.getName();
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
            //     break;

            // case Types.REF:
            //     break;
            // case Types.DATALINK:
            //     break;
            case Types.BOOLEAN:
                typeName = MysqlType.BOOLEAN.getName();
                break;
            // case Types.ROWID:
            //     break;
            // case Types.SQLXML:
            //     break;
            // case Types.REF_CURSOR:
            //     break;

            case microsoft.sql.Types.GEOMETRY:
                typeName = MysqlType.GEOMETRY.getName();
                break;
            case microsoft.sql.Types.GEOGRAPHY:
                typeName = "geography";
                break;
            case microsoft.sql.Types.SQL_VARIANT:
                typeName = "sql_variant";
                break;
            case microsoft.sql.Types.GUID:
                typeName = MysqlType.VARCHAR.getName();
                length = 36;
                break;
            case microsoft.sql.Types.DATETIME:
            case microsoft.sql.Types.SMALLDATETIME:
            case microsoft.sql.Types.DATETIMEOFFSET:
                typeName = MysqlType.DATETIME.getName();
                break;
            case microsoft.sql.Types.MONEY:
                typeName = MysqlType.DECIMAL.getName();
                precision = 19;
                scale = 4;
                break;
            case microsoft.sql.Types.SMALLMONEY:
                typeName = MysqlType.DECIMAL.getName();
                precision = 10;
                scale = 4;
                break;
        }

        MysqlType mysqlType = MysqlType.getByJdbcType(columnInfo.getDataType());
        // jdbc扩展类型
        if (StringUtils.isEmpty(typeName)) {
            // typeName == ""
            switch (mysqlType) {
                case ENUM:
                case SET:
                    if (this.isSameDatabaseVendor()) {
                        // 源端和目标端的数据库类型相同
                        typeName = columnInfo.getColumnType();
                    }
                    break;
                default:
                    typeName = mysqlType.getName();
            }
        }

        sqlBuilder.append(typeName);
        if (mysqlType.isAllowed(MysqlType.FIELD_FLAG_UNSIGNED)) {
            // 是否为无符号
            sqlBuilder.append(" ").append("unsigned");
        }

        if (length != -1) {
            sqlBuilder.append("(").append(length).append(")");
        } else if (precision != -1) {
            sqlBuilder.append("(").append(precision);
            if (scale != -1) {
                sqlBuilder.append(", ").append(scale);
            }
            sqlBuilder.append(")");
        }
    }

    @Override
    public String getRightName(String name) {
        if (this.getDatabaseConfig().isLowerCaseName()) {
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
        return new ArrayList<String>() {
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
     * https://dev.mysql.com/doc/refman/5.6/en/alter-table.html
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

            if (ii.isTableIndexStatistic()) {
                // 统计信息不需要。
                continue;
            }

            if (sBuilder == null) {
                sBuilder = new StringBuilder();
            }
            if (buildIndex == null) {
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
            } else if (IndexTypeEnum.FULLTEXT.toString().equalsIgnoreCase(ii.getIndexTypeDesc())) {
                // 全文索引
                sBuilder.append(ii.getIndexTypeDesc()).append(" INDEX ");
                indexType = SlaveMetaDataEntity.TYPE_FULLTEXT_INDEX;
            } else if(IndexTypeEnum.SPATIAL.toString().equalsIgnoreCase(ii.getIndexTypeDesc())){
                // 空间索引
                sBuilder.append(ii.getIndexTypeDesc()).append(" INDEX ");
                indexType = SlaveMetaDataEntity.TYPE_SPATIAL_INDEX;
            } else {
                // 普通索引
                sBuilder.append("INDEX ");
                indexType = SlaveMetaDataEntity.TYPE_INDEX;
            }

            // [index_name]
            String indexName = ii.getIndexName();
            if (this.getDatabaseConfig().isGenerateName()) {
                // 自动生成
                indexName = this.buildIndexName(ii, ti);
            }

            sBuilder.append(this.getRightName(indexName)).append(" ");

            // [index_type]
            if (ii.getIndexTypeDesc() != null) {
                switch (ii.getIndexTypeDesc()) {
                    case "BTREE":
                    case "HASH":
                        sBuilder.append("USING ").append(ii.getIndexTypeDesc()).append(" ");
                        break;
                    default:
                }
            }

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

    @Override
    public void buildComment(TableInfo ti) {

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
    public String getPaginationSQL(TableInfo ti, int offset, int limit) {

        // 唯一键
        String colNames = this.getTableUniqueKeys(ti.getUniqueKeys());
        if (colNames.length() == 0) {
            // 无唯一键
            // 使用全部列
            colNames = this.getTableOrderColumnNames(ti.getColumns());
        }

        // 只有一个主键或一个唯一键
        if (ti.getUniqueKeys().size() == 1) {
            // 延迟关联
            // https://www.cnblogs.com/wang-meng/p/ae6d1c4a7b553e9a5c8f46b67fb3e3aa.html
            return String.format("SELECT * FROM %s INNER JOIN (SELECT %s FROM %s ORDER BY %s LIMIT %s, %s) AS X USING(%s)",
                    this.getRightName(ti.getTableName()),
                    colNames,
                    this.getRightName(ti.getTableName()),
                    colNames,
                    offset, limit,
                    colNames);
        } else {
            // 多个
            // 普通分页查询
            return String.format("SELECT * FROM %s ORDER BY %s LIMIT %s, %s", this.getRightName(ti.getTableName()), colNames, offset, limit);
        }

    }


    @Override
    public String getDatabaseVendor() {
        return DatabaseVendorEnum.MYSQL.getVendor();
    }

    @Override
    public String getAutoincrement() {
        return "AUTO_INCREMENT";
    }

    @Override
    public void buildPrimaryKeys(TableInfo ti, StringBuilder sqlBuilder) {
        // 考虑到其他数据库存在自动增长的属性，且不是主键的问题。
        // 自动增长的字段需要为主键
        // Incorrect table definition; there can be only one auto column and it must be defined as a key
        // 一个表仅支持一个自动增长的字段。
        if (ti.getPrimaryKeys().size() == 0) {
            return;
        }

        // 获取一个自动增长的列名
        String colName = null;
        for (ColumnInfo c : ti.getColumns()) {
            if (c.isAutoincrement()) {
                colName = c.getColumnName();
                break;
            }
        }

        // 是否为有效的主键列
        boolean valid = false;
        if (colName != null) {
            for (PrimaryKey pk : ti.getPrimaryKeys()) {
                if (pk.getColumnName().equals(colName)) {
                    valid = true;
                    break;
                }
            }
        } else {
            valid = true;
        }

        sqlBuilder.append("PRIMARY KEY (");
        // 再次排序
        ti.getPrimaryKeys().sort(Comparator.comparing(PrimaryKey::getKeySeq));

        if (valid) {
            ti.getPrimaryKeys().forEach(pk -> sqlBuilder.append(getRightName(pk.getColumnName())).append(","));
        } else {
            // 从主键列表中没有找到主键，自动增长需要为主键。
            sqlBuilder.append(getRightName(colName)).append(",");

            // 将原来的主键修改为唯一约束
            StringBuilder sBuilder = new StringBuilder("ALTER TABLE ");
            sBuilder.append(this.getRightName(ti.getTableName()));
            sBuilder.append(" ADD CONSTRAINT UNIQUE KEY (");
            StringBuilder sb0 = new StringBuilder();
            ti.getPrimaryKeys().forEach(pk -> {
                sBuilder.append(getRightName(pk.getColumnName())).append(",");
                sb0.append(getRightName(pk.getColumnName())).append(",");
            });
            sBuilder.delete(sBuilder.length() - 1, sBuilder.length()).append(")");

            log.warn("{}, MySQL: Table {} primary key {} change to column {}!!!!",
                    this.getDatabaseConfig().getName(),
                    this.getRightName(ti.getTableName()),
                    sb0.delete(sb0.length() - 1, sb0.length()).toString(),
                    this.getRightName(colName));


            // 保存
            this.databaseDao.insertMetadata(new SlaveMetaDataEntity(ti.getTableName(), SlaveMetaDataEntity.TYPE_UNIQUE_INDEX, sBuilder.toString()));
        }

        sqlBuilder.delete(sqlBuilder.length() - 1, sqlBuilder.length()).append("),");
    }

    @Override
    public void changeBatchArgs(List<Object[]> batchArgs, TableInfo ti) {

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
    protected String buildInsertSQL(TableInfo ti) {
        // 生成sql语句：
        // insert into T (col1, col2, col3, ...) values (?, ?, ?, ...)
        final StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ").append(this.getRightName(ti.getTableName())).append(" (");
        final StringBuilder sqlPlaceHolderBuilder = new StringBuilder();

        for (ColumnInfo c : ti.getColumns()) {
            sqlBuilder.append(this.getRightName(c.getColumnName())).append(",");
            if(MysqlType.YEAR.getName().equals(c.getTypeName()) && Types.DATE == c.getDataType()){
                // 年份
                sqlPlaceHolderBuilder.append("year(?),");
            } else {
                sqlPlaceHolderBuilder.append("?,");
            }
        }

        sqlBuilder.delete(sqlBuilder.length() - 1, sqlBuilder.length()).append(") VALUES (");
        sqlPlaceHolderBuilder.delete(sqlPlaceHolderBuilder.length() - 1, sqlPlaceHolderBuilder.length());
        sqlBuilder.append(sqlPlaceHolderBuilder).append(")");
        return sqlBuilder.toString();
    }

    @Override
    public DatabaseComparator getComparator() {
        return this.databaseComparator;
    }


}
