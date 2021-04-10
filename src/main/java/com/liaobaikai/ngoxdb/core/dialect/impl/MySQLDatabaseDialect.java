package com.liaobaikai.ngoxdb.core.dialect.impl;

import com.liaobaikai.ngoxdb.bean.ColumnType;
import com.liaobaikai.ngoxdb.bean.info.ColumnInfo;
import com.liaobaikai.ngoxdb.core.constant.JdbcDataType;
import com.liaobaikai.ngoxdb.core.dialect.BasicDatabaseDialect;
import com.liaobaikai.ngoxdb.core.enums.func.DateTypeFunctionEnum;
import com.liaobaikai.ngoxdb.core.enums.func.MathFunctionEnum;
import com.liaobaikai.ngoxdb.core.enums.func.StringFunctionEnum;
import com.liaobaikai.ngoxdb.core.func.impl.NoArgumentSQLFunction;
import com.liaobaikai.ngoxdb.core.func.impl.StandardSQLFunction;
import com.mysql.cj.MysqlType;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * MySQL数据库方言
 *
 * @author baikai.liao
 * @Time 2021-03-13 01:05:18
 */
@Service("MySQLDatabaseDialect")
public class MySQLDatabaseDialect extends BasicDatabaseDialect {

    public MySQLDatabaseDialect() {

        // 注册类型
        this.registerColumnType(JdbcDataType.LONGVARBINARY, "longblob");

        this.registerColumnType(JdbcDataType.MONEY, "numeric");
        this.registerColumnType(JdbcDataType.SMALLMONEY, "numeric");

        this.registerColumnType(JdbcDataType.LONGVARCHAR, "text");
        this.registerColumnType(JdbcDataType.LONGNVARCHAR, "text");

        this.registerColumnType(JdbcDataType.NCLOB, "text");
        this.registerColumnType(JdbcDataType.CLOB, "text");

        this.registerColumnType(JdbcDataType.LONGTEXT, "longtext");
        this.registerColumnType(JdbcDataType.MEDIUMTEXT, "mediumtext");

        this.registerColumnType(JdbcDataType.LONGBLOB, "longblob");
        this.registerColumnType(JdbcDataType.MEDIUMBLOB, "mediumblob");


        this.registerColumnType(JdbcDataType.GUID, "char");

        this.registerColumnType(JdbcDataType.TIME_WITH_TIMEZONE, "time");
        this.registerColumnType(JdbcDataType.TIMESTAMP_WITH_TIMEZONE, "timestamp");

        this.registerColumnType(JdbcDataType.TIMESTAMPTZ, "timestamp");
        this.registerColumnType(JdbcDataType.TIMESTAMPLTZ, "timestamp");

        this.registerColumnType(JdbcDataType.DATETIME, "datetime");
        this.registerColumnType(JdbcDataType.DATETIMEOFFSET, "datetime");
        this.registerColumnType(JdbcDataType.SMALLDATETIME, "datetime");

        this.registerColumnType(JdbcDataType.ENUM, "enum");
        this.registerColumnType(JdbcDataType.SET, "set");
        this.registerColumnType(JdbcDataType.JSON, "json");
        this.registerColumnType(JdbcDataType.YEAR, "year");

        // 注册函数
        // 数学函数
        // https://dev.mysql.com/doc/refman/8.0/en/numeric-functions.html
        // 不支持：
        // CONV()
        // CRC32()
        // DIV
        // LOG2()
        this.registerFunction(MathFunctionEnum.abs, new StandardSQLFunction("abs"));
        this.registerFunction(MathFunctionEnum.ceil, new StandardSQLFunction("ceil"));
        this.registerFunction(MathFunctionEnum.ceiling, new StandardSQLFunction("ceiling"));
        this.registerFunction(MathFunctionEnum.degrees, new StandardSQLFunction("degrees"));
        this.registerFunction(MathFunctionEnum.exp, new StandardSQLFunction("exp"));
        this.registerFunction(MathFunctionEnum.floor, new StandardSQLFunction("floor"));
        this.registerFunction(MathFunctionEnum.ln, new StandardSQLFunction("ln"));
        this.registerFunction(MathFunctionEnum.log, new StandardSQLFunction("log"));
        this.registerFunction(MathFunctionEnum.log10, new StandardSQLFunction("log10"));
        this.registerFunction(MathFunctionEnum.mod, new StandardSQLFunction("mod"));
        this.registerFunction(MathFunctionEnum.pi, new StandardSQLFunction("pi"));
        this.registerFunction(MathFunctionEnum.power,
                new StandardSQLFunction("pow"),
                new StandardSQLFunction("power"));
        this.registerFunction(MathFunctionEnum.radians, new StandardSQLFunction("radians"));
        this.registerFunction(MathFunctionEnum.round, new StandardSQLFunction("round"));
        this.registerFunction(MathFunctionEnum.sign, new StandardSQLFunction("sign"));
        this.registerFunction(MathFunctionEnum.sqrt, new StandardSQLFunction("sqrt"));
        this.registerFunction(MathFunctionEnum.trunc, new StandardSQLFunction("truncate"));
        this.registerFunction(MathFunctionEnum.random, new StandardSQLFunction("rand"));
        this.registerFunction(MathFunctionEnum.acos, new StandardSQLFunction("acos"));
        this.registerFunction(MathFunctionEnum.asin, new StandardSQLFunction("asin"));
        this.registerFunction(MathFunctionEnum.atan, new StandardSQLFunction("atan"));
        this.registerFunction(MathFunctionEnum.atan2, new StandardSQLFunction("atan2"));
        this.registerFunction(MathFunctionEnum.cos, new StandardSQLFunction("cos"));
        this.registerFunction(MathFunctionEnum.cot, new StandardSQLFunction("cot"));
        this.registerFunction(MathFunctionEnum.sin, new StandardSQLFunction("sin"));
        this.registerFunction(MathFunctionEnum.tan, new StandardSQLFunction("tan"));

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
        // WEEK()
        // WEEKDAY()
        // WEEKOFYEAR()
        // YEAR()
        // YEARWEEK()

        this.registerFunction(DateTypeFunctionEnum.to_char, new StandardSQLFunction("time_format"));
        this.registerFunction(DateTypeFunctionEnum.to_date, new StandardSQLFunction("str_to_date"));
        this.registerFunction(DateTypeFunctionEnum.to_timestamp, new StandardSQLFunction("from_unixtime"));    // 数字转timestamp

        this.registerFunction(DateTypeFunctionEnum.age, new StandardSQLFunction("timestampdiff"));
        this.registerFunction(DateTypeFunctionEnum.clock_timestamp,
                new NoArgumentSQLFunction("now", JdbcDataType.TIMESTAMP));   // Current date and time (changes during statement execution);
        this.registerFunction(DateTypeFunctionEnum.current_date,
                new NoArgumentSQLFunction("curdate", JdbcDataType.DATE),
                new NoArgumentSQLFunction("current_date", JdbcDataType.DATE),
                new NoArgumentSQLFunction("current_date", JdbcDataType.DATE, false));

        this.registerFunction(DateTypeFunctionEnum.current_time,
                new NoArgumentSQLFunction("curtime", JdbcDataType.TIME),
                new NoArgumentSQLFunction("current_time", JdbcDataType.TIME),
                new NoArgumentSQLFunction("current_time", JdbcDataType.TIME, false));

        this.registerFunction(DateTypeFunctionEnum.current_timestamp,
                new NoArgumentSQLFunction("current_timestamp", JdbcDataType.TIMESTAMP),
                new NoArgumentSQLFunction("current_timestamp", JdbcDataType.TIMESTAMP, false));

        this.registerFunction(DateTypeFunctionEnum.extract, new StandardSQLFunction("extract"));
        this.registerFunction(DateTypeFunctionEnum.localtime, new NoArgumentSQLFunction("sysdate", JdbcDataType.TIMESTAMP));  // Synonym for NOW()
        this.registerFunction(DateTypeFunctionEnum.localtimestamp, new NoArgumentSQLFunction("sysdate", JdbcDataType.TIMESTAMP));  // Synonym for NOW()
        this.registerFunction(DateTypeFunctionEnum.make_date, new StandardSQLFunction("makedate"));    // 存在差异。。。
        this.registerFunction(DateTypeFunctionEnum.make_time, new StandardSQLFunction("maketime"));
        this.registerFunction(DateTypeFunctionEnum.make_timestamp, new StandardSQLFunction("timestamp"));
        this.registerFunction(DateTypeFunctionEnum.make_timestamptz, new StandardSQLFunction("timestamp"));

        // https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html
        this.registerFunction(DateTypeFunctionEnum.now,
                new NoArgumentSQLFunction("now", JdbcDataType.TIMESTAMP),
                new NoArgumentSQLFunction("localtime", JdbcDataType.TIME),
                new NoArgumentSQLFunction("localtime", JdbcDataType.TIME, false),
                new NoArgumentSQLFunction("localtimestamp", JdbcDataType.TIMESTAMP),
                new NoArgumentSQLFunction("localtimestamp", JdbcDataType.TIMESTAMP, false),
                new NoArgumentSQLFunction("current_timestamp", JdbcDataType.TIMESTAMP),
                new NoArgumentSQLFunction("current_timestamp", JdbcDataType.TIMESTAMP, false));    // Current date and time (start of current transaction);

        this.registerFunction(DateTypeFunctionEnum.statement_timestamp, new NoArgumentSQLFunction("current_timestamp", JdbcDataType.TIMESTAMP));   // Current date and time (start of current statement);
        this.registerFunction(DateTypeFunctionEnum.timeofday, new NoArgumentSQLFunction("utc_time", JdbcDataType.TIME));   //
        this.registerFunction(DateTypeFunctionEnum.transaction_timestamp, new NoArgumentSQLFunction("current_timestamp", JdbcDataType.TIMESTAMP));  // Current date and time (start of current transaction);

        this.registerFunction(DateTypeFunctionEnum.last_day, new StandardSQLFunction("last_day"));  // Current date and time (start of current transaction);

        // 带时区
        this.registerFunction(DateTypeFunctionEnum.datetime_offset, new NoArgumentSQLFunction("current_timestamp", JdbcDataType.TIMESTAMP));

        // utc类型
        this.registerFunction(DateTypeFunctionEnum.utc_datetime, new NoArgumentSQLFunction("utc_timestamp", JdbcDataType.TIMESTAMP));
        this.registerFunction(DateTypeFunctionEnum.utc_date, new NoArgumentSQLFunction("utc_date", JdbcDataType.DATE));
        this.registerFunction(DateTypeFunctionEnum.utc_time, new NoArgumentSQLFunction("utc_time", JdbcDataType.TIME));

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

        this.registerFunction(StringFunctionEnum.bit_length, new StandardSQLFunction("bit_length"));
        this.registerFunction(StringFunctionEnum.char_length, new StandardSQLFunction("char_length"));
        this.registerFunction(StringFunctionEnum.character_length, new StandardSQLFunction("character_length"));
        this.registerFunction(StringFunctionEnum.lower,
                new StandardSQLFunction("lcase"),
                new StandardSQLFunction("lower"));
        this.registerFunction(StringFunctionEnum.octet_length, new StandardSQLFunction("octet_length"));
        this.registerFunction(StringFunctionEnum.position, new StandardSQLFunction("position"));
        this.registerFunction(StringFunctionEnum.substring,
                new StandardSQLFunction("substring"),
                new StandardSQLFunction("mid"));    // MID(str,pos,len) is a synonym for SUBSTRING(str,pos,len).
        this.registerFunction(StringFunctionEnum.trim, new StandardSQLFunction("trim"));
        this.registerFunction(StringFunctionEnum.upper,
                new StandardSQLFunction("ucase"),
                new StandardSQLFunction("upper"));
        this.registerFunction(StringFunctionEnum.ascii, new StandardSQLFunction("ascii"));
        this.registerFunction(StringFunctionEnum.chr, new StandardSQLFunction("char"));
        this.registerFunction(StringFunctionEnum.concat, new StandardSQLFunction("concat"));
        this.registerFunction(StringFunctionEnum.concat_ws, new StandardSQLFunction("concat_ws"));
        this.registerFunction(StringFunctionEnum.left, new StandardSQLFunction("left"));
        this.registerFunction(StringFunctionEnum.length, new StandardSQLFunction("length"));
        this.registerFunction(StringFunctionEnum.lpad, new StandardSQLFunction("lpad"));
        this.registerFunction(StringFunctionEnum.ltrim, new StandardSQLFunction("ltrim"));
        this.registerFunction(StringFunctionEnum.md5, new StandardSQLFunction("md5"));
        this.registerFunction(StringFunctionEnum.repeat, new StandardSQLFunction("repeat"));
        this.registerFunction(StringFunctionEnum.replace, new StandardSQLFunction("replace"));
        this.registerFunction(StringFunctionEnum.reverse, new StandardSQLFunction("reverse"));
        this.registerFunction(StringFunctionEnum.right, new StandardSQLFunction("right"));
        this.registerFunction(StringFunctionEnum.rpad, new StandardSQLFunction("rpad"));
        this.registerFunction(StringFunctionEnum.rtrim, new StandardSQLFunction("rtrim"));
        this.registerFunction(StringFunctionEnum.strpos, new StandardSQLFunction("instr"));
        this.registerFunction(StringFunctionEnum.substr, new StandardSQLFunction("substr"));
        this.registerFunction(StringFunctionEnum.to_hex, new StandardSQLFunction("hex"));

        // bytes
        this.registerFunction(StringFunctionEnum.uuid, new NoArgumentSQLFunction("uuid", JdbcDataType.VARCHAR));

    }

    @Override
    public char openQuote() {
        return '`';
    }

    @Override
    public char closeQuote() {
        return '`';
    }

    @Override
    public boolean supportsCommentOnBuildTable() {
        return true;
    }

    @Override
    public boolean supportsCollationOnBuildTable() {
        return true;
    }

    @Override
    public String getCollationString(String collationName) {
        return "COLLATE " + collationName;
    }

    @Override
    public boolean supportsCharsetOnBuildTable() {
        return true;
    }

    @Override
    public String getCharsetString(String charsetName) {
        return "CHARACTER SET " + charsetName;
    }

    @Override
    public String getTableCommentString(String tableName, String tableComment) {
        return String.format(" comment='%s' ", tableComment);
    }

    @Override
    public String getColumnCommentString(String tableName, String columnName, String columnComment) {
        return String.format(" comment '%s' ", columnComment);
    }

    @Override
    public boolean supportsDropForeignKey() {
        return true;
    }

    @Override
    public boolean supportsUnsignedDecimal() {
        return true;
    }

    @Override
    public String getDropForeignKeyString() {
        return " drop foreign key ";
    }

    @Override
    public boolean supportsPartitionBy() {
        return true;
    }

    @Override
    public boolean supportsIfExistsBeforeTableName() {
        return true;
    }

    @Override
    public boolean supportsIdentityColumns() {
        return true;
    }

    @Override
    public boolean doesIdentityMustPrimaryKey() {
        return true;
    }

    @Override
    public String getIdentityColumnString() {
        return "auto_increment";
    }

    @Override
    public boolean supportsLimit() {
        return true;
    }

    @Override
    public boolean supportsTruncateTable() {
        return true;
    }

    @Override
    public int getMaximumPrecision(int jdbcDataType) {
        switch (jdbcDataType) {
            case JdbcDataType.NUMERIC:
            case JdbcDataType.DECIMAL:
                return MysqlType.DECIMAL.getPrecision().intValue();
            case JdbcDataType.CHAR:
            case JdbcDataType.NCHAR:
                return MysqlType.CHAR.getPrecision().intValue();
            case JdbcDataType.VARCHAR:
            case JdbcDataType.NVARCHAR:
                return MysqlType.VARCHAR.getPrecision().intValue();
            case JdbcDataType.BIT:
                return 1;
            case JdbcDataType.BINARY:
                return MysqlType.BINARY.getPrecision().intValue();
            case JdbcDataType.VARBINARY:
                return MysqlType.VARBINARY.getPrecision().intValue();
            default:
                return 0;
        }
    }

    @Override
    public int getMinimumPrecision(int jdbcDataType) {
        switch (jdbcDataType) {
            case JdbcDataType.NUMERIC:
            case JdbcDataType.DECIMAL:
            case JdbcDataType.CHAR:
            case JdbcDataType.NCHAR:
            case JdbcDataType.VARCHAR:
            case JdbcDataType.NVARCHAR:
            case JdbcDataType.BIT:
            case JdbcDataType.BINARY:
                return 1;
            default:
                return 0;
        }
    }

    @Override
    public int getMaximumScale(int jdbcDataType) {
        switch (jdbcDataType) {
            case JdbcDataType.NUMERIC:
            case JdbcDataType.DECIMAL:
                return 38;
            default:
                return 0;
        }
    }

    @Override
    public int getMinimumScale(int jdbcDataType) {
        switch (jdbcDataType) {
            case JdbcDataType.CHAR:
            case JdbcDataType.NCHAR:
            case JdbcDataType.VARCHAR:
            case JdbcDataType.NVARCHAR:
            case JdbcDataType.BIT:
            case JdbcDataType.BINARY:
                return 1;
            default:
                return 0;
        }
    }

    @Override
    public int getRightDataTypeForVarchar(int columnSize, boolean[] doesIgnoredLength) {
        int maximumPrecision = this.getMaximumPrecision(JdbcDataType.VARCHAR);
        if (columnSize > maximumPrecision) {      // > 65535
            doesIgnoredLength[0] = true;
            if (columnSize > MysqlType.MEDIUMTEXT.getPrecision()) {  // > 16777215
                // (16777215, 4294967295]
                return JdbcDataType.LONGTEXT;
            } else if (columnSize > MysqlType.TEXT.getPrecision()) { // > 65535
                return JdbcDataType.MEDIUMTEXT;
            }
        }
        return JdbcDataType.VARCHAR;
    }

    @Override
    public int getRightDataTypeForNVarchar(int columnSize, boolean[] doesIgnoredLength) {
        return this.getRightDataTypeForVarchar(columnSize, doesIgnoredLength);
    }

    @Override
    public int getRightDataTypeForChar(int columnSize, boolean[] doesIgnoredLength) {
        int maximumPrecision = this.getMaximumPrecision(JdbcDataType.CHAR);
        if (columnSize > maximumPrecision) {  // > 255
            return this.getRightDataTypeForVarchar(columnSize, doesIgnoredLength);
        }
        // <= 255
        return JdbcDataType.CHAR;
    }

    @Override
    public int getRightDataTypeForNChar(int columnSize, boolean[] doesIgnoredLength) {
        return this.getRightDataTypeForChar(columnSize, doesIgnoredLength);
    }

    @Override
    public int getRightDataTypeForBinary(int columnSize, boolean[] doesIgnoredLength) {
        int maximumPrecision = this.getMaximumPrecision(JdbcDataType.BINARY);
        if (columnSize > maximumPrecision) {  // > 255
            return this.getRightDataTypeForVarBinary(columnSize, doesIgnoredLength);
        }
        return JdbcDataType.BINARY;
    }

    @Override
    public int getRightDataTypeForVarBinary(int columnSize, boolean[] doesIgnoredLength) {
        int maximumPrecision = this.getMaximumPrecision(JdbcDataType.VARBINARY);
        if (columnSize > maximumPrecision) {  // > 65535
            doesIgnoredLength[0] = true;
            if (columnSize > MysqlType.MEDIUMBLOB.getPrecision()) {  // > 16777215
                // (16777215, 4294967295]
                return JdbcDataType.LONGBLOB;
            } else if (columnSize > MysqlType.BLOB.getPrecision()) { // > 65535
                return JdbcDataType.MEDIUMBLOB;
            }
        }
        return JdbcDataType.VARBINARY;
    }

    @Override
    public boolean supportsDecimalToInt() {
        return true;
    }

    @Override
    public boolean supportsYearToDate() {
        return false;
    }

    @Override
    public boolean supportsJoin() {
        return true;
    }

    @Override
    public Object getSqlGenericType(Object src) {
        return src;
    }

    @Override
    public boolean supportsFullTextIndex() {
        return true;
    }

    @Override
    public String getFullTextIndexString() {
        return "fulltext";
    }

    @Override
    public boolean supportsSpatialIndex() {
        return true;
    }

    @Override
    public void registerColumnTypeForYear(ColumnType ct, ColumnInfo ci) {
        ct.setTypeName(ci.getColumnType());
    }

    @Override
    public void registerColumnTypeForJson(ColumnType ct, ColumnInfo ci) {
        ct.setTypeName(ci.getColumnType());
    }

    @Override
    public void registerColumnTypeForSet(ColumnType ct, ColumnInfo ci) {
        ct.setTypeName(ci.getColumnType());
    }

    @Override
    public void registerColumnTypeForEnum(ColumnType ct, ColumnInfo ci) {
        ct.setTypeName(ci.getColumnType());
    }

    @Override
    public void registerColumnTypeForFixedChar(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForFixedChar(ct, ci);
    }

    @Override
    public void registerColumnTypeForBinaryDouble(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForBinaryDouble(ct, ci);
    }

    @Override
    public void registerColumnTypeForBinaryFloat(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForBinaryFloat(ct, ci);
    }

    @Override
    public void registerColumnTypeForPlsqlIndexTable(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForPlsqlIndexTable(ct, ci);
    }

    @Override
    public void registerColumnTypeForOpaque(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForOpaque(ct, ci);
    }

    @Override
    public void registerColumnTypeForBfile(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForBfile(ct, ci);
    }

    @Override
    public void registerColumnTypeForCursor(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForCursor(ct, ci);
    }

    @Override
    public void registerColumnTypeForIntervalDS(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForIntervalDS(ct, ci);
    }

    @Override
    public void registerColumnTypeForIntervalYM(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForIntervalYM(ct, ci);
    }

    @Override
    public void registerColumnTypeForTimestampLTZ(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForTimestampLTZ(ct, ci);
    }

    @Override
    public void registerColumnTypeForTimestampTZ(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForTimestampTZ(ct, ci);
    }

    @Override
    public void registerColumnTypeForJavaStruct(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForJavaStruct(ct, ci);
    }

    @Override
    public void registerColumnTypeForGeography(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForGeography(ct, ci);
    }

    @Override
    public void registerColumnTypeForGeometry(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForGeometry(ct, ci);
    }

    @Override
    public void registerColumnTypeForSqlVariant(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForSqlVariant(ct, ci);
    }

    @Override
    public void registerColumnTypeForGuid(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForGuid(ct, ci);
    }

    @Override
    public void registerColumnTypeForSmallMoney(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForSmallMoney(ct, ci);
    }

    @Override
    public void registerColumnTypeForMoney(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForMoney(ct, ci);
    }

    @Override
    public void registerColumnTypeForSmallDateTime(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForSmallDateTime(ct, ci);
    }

    @Override
    public void registerColumnTypeForDateTime(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForDateTime(ct, ci);
    }

    @Override
    public void registerColumnTypeForStructured(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForStructured(ct, ci);
    }

    @Override
    public void registerColumnTypeForDateTimeOffset(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForDateTimeOffset(ct, ci);
    }

    @Override
    public void registerColumnTypeForTimestampZ(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForTimestampZ(ct, ci);
    }

    @Override
    public void registerColumnTypeForTimeZ(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForTimeZ(ct, ci);
    }

    @Override
    public void registerColumnTypeForRefCursor(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForRefCursor(ct, ci);
    }

    @Override
    public void registerColumnTypeForSqlXml(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForSqlXml(ct, ci);
    }

    @Override
    public void registerColumnTypeForNClob(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForNClob(ct, ci);
    }

    @Override
    public void registerColumnTypeForLongNVarchar(ColumnType ct, ColumnInfo ci) {
        this.registerColumnTypeForLongVarchar(ct, ci);
    }

    @Override
    public void registerColumnTypeForNVarchar(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForNVarchar(ct, ci);
    }

    @Override
    public void registerColumnTypeForNChar(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForNChar(ct, ci);
    }

    @Override
    public void registerColumnTypeForRowId(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForRowId(ct, ci);
    }

    @Override
    public void registerColumnTypeForBoolean(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForBoolean(ct, ci);
    }

    @Override
    public void registerColumnTypeForDataLink(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForDataLink(ct, ci);
    }

    @Override
    public void registerColumnTypeForRef(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForRef(ct, ci);
    }

    @Override
    public void registerColumnTypeForClob(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForClob(ct, ci);
    }

    @Override
    public void registerColumnTypeForBlob(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForBlob(ct, ci);
    }

    @Override
    public void registerColumnTypeForArray(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForArray(ct, ci);
    }

    @Override
    public void registerColumnTypeForStruct(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForStruct(ct, ci);
    }

    @Override
    public void registerColumnTypeForDistinct(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForDistinct(ct, ci);
    }

    @Override
    public void registerColumnTypeForJavaObject(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForJavaObject(ct, ci);
    }

    @Override
    public void registerColumnTypeForOther(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForOther(ct, ci);
    }

    @Override
    public void registerColumnTypeForNull(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForNull(ct, ci);
    }

    @Override
    public void registerColumnTypeForLongVarbinary(ColumnType ct, ColumnInfo ci) {
        if (ci.getColumnSize() > MysqlType.MEDIUMBLOB.getPrecision()) {  // > 16777215
            // (16777215, 4294967295]
            ct.setJdbcDataType(JdbcDataType.LONGBLOB);
        } else if (ci.getColumnSize() > MysqlType.BLOB.getPrecision()) { // > 65535
            ct.setJdbcDataType(JdbcDataType.MEDIUMBLOB);
        } else {
            ct.setJdbcDataType(JdbcDataType.BLOB);
        }
    }

    @Override
    public void registerColumnTypeForVarbinary(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForVarbinary(ct, ci);
    }

    @Override
    public void registerColumnTypeForBinary(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForBinary(ct, ci);
    }

    @Override
    public void registerColumnTypeForTimestamp(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForTimestamp(ct, ci);
    }

    @Override
    public void registerColumnTypeForTime(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForTime(ct, ci);
    }

    @Override
    public void registerColumnTypeForDate(ColumnType ct, ColumnInfo ci) {
        if (ci.getDataType() == JdbcDataType.YEAR) {
            // 年份
            ct.setTypeName(ci.getColumnType());
            return;
        }
        super.registerColumnTypeForDate(ct, ci);
    }

    @Override
    public void registerColumnTypeForLongVarchar(ColumnType ct, ColumnInfo ci) {
        if (ci.getDataType() == JdbcDataType.JSON) {
            ct.setColumnName(ci.getColumnType());
        } else if (ci.getColumnSize() > MysqlType.MEDIUMTEXT.getPrecision()) {  // > 16777215
            // (16777215, 4294967295]
            ct.setJdbcDataType(JdbcDataType.LONGTEXT);
        } else if (ci.getColumnSize() > MysqlType.TEXT.getPrecision()) { // > 65535
            ct.setJdbcDataType(JdbcDataType.MEDIUMTEXT);
        } else {
            ct.setTypeName(MysqlType.TEXT.getName());
        }
    }

    @Override
    public void registerColumnTypeForVarchar(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForVarchar(ct, ci);
    }

    @Override
    public void registerColumnTypeForChar(ColumnType ct, ColumnInfo ci) {
        switch (ci.getDataType()) {
            case JdbcDataType.ENUM:
            case JdbcDataType.SET:
                ct.setTypeName(ci.getColumnType());
                break;
            case JdbcDataType.CHAR:
                super.registerColumnTypeForChar(ct, ci);
        }
    }

    @Override
    public void registerColumnTypeForDecimal(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForDecimal(ct, ci);
    }

    @Override
    public void registerColumnTypeForNumeric(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForNumeric(ct, ci);
    }

    @Override
    public void registerColumnTypeForDouble(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForDouble(ct, ci);
    }

    @Override
    public void registerColumnTypeForReal(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForReal(ct, ci);
    }

    @Override
    public void registerColumnTypeForFloat(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForFloat(ct, ci);
    }

    @Override
    public void registerColumnTypeForBigInt(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForBigInt(ct, ci);
    }

    @Override
    public void registerColumnTypeForInt(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForInt(ct, ci);
    }

    @Override
    public void registerColumnTypeForSmallint(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForSmallint(ct, ci);
    }

    @Override
    public void registerColumnTypeForTinyint(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForTinyint(ct, ci);
    }

    @Override
    public void registerColumnTypeForBit(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForBit(ct, ci);
    }

    @Override
    protected void handleBuildInsertPreparedSql(final ColumnInfo c, String finalColumnName, final StringBuilder sqlBuilder, final StringBuilder sqlPlaceHolderBuilder) {


        sqlBuilder.append(this.toLookupName(finalColumnName)).append(",");

        if (JdbcDataType.YEAR == c.getDataType()) {
            // 年份
            sqlPlaceHolderBuilder.append("year(?),");
        } else {
            sqlPlaceHolderBuilder.append("?,");
        }
    }
}
