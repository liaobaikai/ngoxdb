package com.liaobaikai.ngoxdb.core.dialect.impl;

import com.liaobaikai.ngoxdb.bean.ColumnType;
import com.liaobaikai.ngoxdb.bean.info.ColumnInfo;
import com.liaobaikai.ngoxdb.core.constant.JdbcDataType;
import com.liaobaikai.ngoxdb.core.dialect.BasicDatabaseDialect;
import com.liaobaikai.ngoxdb.core.enums.func.DateTypeFunctionEnum;
import com.liaobaikai.ngoxdb.core.enums.func.MathFunctionEnum;
import com.liaobaikai.ngoxdb.core.enums.func.StringFunctionEnum;
import com.liaobaikai.ngoxdb.core.func.SQLFunction;
import com.liaobaikai.ngoxdb.core.func.impl.NoArgumentSQLFunction;
import com.liaobaikai.ngoxdb.core.func.impl.StandardSQLFunction;
import org.springframework.stereotype.Service;

/**
 * 达梦数据库方言
 *
 * @author baikai.liao
 * @Time 2021-03-14 10:47:30
 */
@Service
public class DmDatabaseDialect extends BasicDatabaseDialect {

    private int charMaxLength = 1;

    // Column max length in CHAR or BYTE, 0: in BYTE, 1: in CHAR
    private int lengthInChar;

    public DmDatabaseDialect() {

        // 《DM8 SQL语言使用手册.pdf》
        //  1.4 DM_SQL 所支持的数据类型

        // 数据类型
        this.registerColumnType(JdbcDataType.BIT, "BIT");
        this.registerColumnType(JdbcDataType.BOOLEAN, "BIT");

        this.registerColumnType(JdbcDataType.BINARY, "BINARY");
        this.registerColumnType(JdbcDataType.VARBINARY, "VARBINARY");
        this.registerColumnType(JdbcDataType.LONGVARBINARY, "IMAGE");
        this.registerColumnType(JdbcDataType.BLOB, "BLOB");

        this.registerColumnType(JdbcDataType.TINYINT, "TINYINT");
        this.registerColumnType(JdbcDataType.SMALLINT, "SMALLINT");
        this.registerColumnType(JdbcDataType.INTEGER, "INTEGER");
        this.registerColumnType(JdbcDataType.BIGINT, "BIGINT");

        this.registerColumnType(JdbcDataType.FLOAT, "FLOAT");
        this.registerColumnType(JdbcDataType.REAL, "REAL");
        this.registerColumnType(JdbcDataType.DOUBLE, "DOUBLE");

        this.registerColumnType(JdbcDataType.DECIMAL, "DECIMAL");
        this.registerColumnType(JdbcDataType.NUMERIC, "NUMERIC");

        this.registerColumnType(JdbcDataType.MONEY, "NUMERIC");
        this.registerColumnType(JdbcDataType.SMALLMONEY, "NUMERIC");


        this.registerColumnType(JdbcDataType.VARCHAR, "VARCHAR");
        this.registerColumnType(JdbcDataType.NVARCHAR, "VARCHAR");

        this.registerColumnType(JdbcDataType.LONGVARCHAR, "TEXT");
        this.registerColumnType(JdbcDataType.LONGNVARCHAR, "TEXT");
        this.registerColumnType(JdbcDataType.CLOB, "TEXT");
        this.registerColumnType(JdbcDataType.NCLOB, "TEXT");

        this.registerColumnType(JdbcDataType.GUID, "CHAR");

        this.registerColumnType(JdbcDataType.TIME, "DATE");
        this.registerColumnType(JdbcDataType.DATE, "DATE");

        // 语法中，TIMESTAMP 也可以写为 DATETIME。
        // DM 支持两种时区类型:标准时区类型和本地时区类型。
        this.registerColumnType(JdbcDataType.TIME_WITH_TIMEZONE, "TIME WITH TIME ZONE");
        this.registerColumnType(JdbcDataType.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP WITH TIME ZONE");

        this.registerColumnType(JdbcDataType.TIMESTAMPTZ, "TIMESTAMP WITH TIME ZONE");
        this.registerColumnType(JdbcDataType.TIMESTAMPLTZ, "TIMESTAMP WITH LOCAL TIME ZONE");

        this.registerColumnType(JdbcDataType.DATETIME, "DATETIME");
        this.registerColumnType(JdbcDataType.DATETIMEOFFSET, "DATETIME");
        this.registerColumnType(JdbcDataType.SMALLDATETIME, "DATETIME");

        this.registerColumnType(JdbcDataType.ENUM, "VARCHAR");
        this.registerColumnType(JdbcDataType.SET, "VARCHAR");
        this.registerColumnType(JdbcDataType.JSON, "JSON");
        this.registerColumnType(JdbcDataType.YEAR, "DATE");

        // 注册函数
        // 《DM8 SQL语言使用手册.pdf》 第8章 函数
        // 不支持
        // 09 COSH(n)
        // 14 GREATEST(n1,n2,n3)
        // 15 GREAT (n1,n2)
        // 16 LEAST(n1,n2,n3)
        // 28 SINH(n)
        // 31 TANH(n)
        // 32 TO_NUMBER (char [,fmt])
        // 33 TRUNC(n[,m])
        // 34 TRUNCATE(n[,m])
        // 35 TO_CHAR(n [, 'nls' ] ])

        // 数学函数
        this.registerFunction(MathFunctionEnum.abs, new StandardSQLFunction("abs"));
        this.registerFunction(MathFunctionEnum.ceil, new StandardSQLFunction("ceil"));
        this.registerFunction(MathFunctionEnum.ceiling, new StandardSQLFunction("ceiling"));
        this.registerFunction(MathFunctionEnum.cot, new StandardSQLFunction("cot"));
        this.registerFunction(MathFunctionEnum.degrees, new StandardSQLFunction("degrees"));
        this.registerFunction(MathFunctionEnum.exp, new StandardSQLFunction("exp"));
        this.registerFunction(MathFunctionEnum.floor, new StandardSQLFunction("floor"));
        this.registerFunction(MathFunctionEnum.ln, new StandardSQLFunction("ln"));
        this.registerFunction(MathFunctionEnum.log, new StandardSQLFunction("log"));
        this.registerFunction(MathFunctionEnum.log10, new StandardSQLFunction("log10"));
        this.registerFunction(MathFunctionEnum.mod, new StandardSQLFunction("mod"));
        this.registerFunction(MathFunctionEnum.pi, new StandardSQLFunction("pi"));
        this.registerFunction(MathFunctionEnum.power, new StandardSQLFunction("power"));
        this.registerFunction(MathFunctionEnum.round, new StandardSQLFunction("round"));
        this.registerFunction(MathFunctionEnum.sign, new StandardSQLFunction("sign", JdbcDataType.INTEGER));
        this.registerFunction(MathFunctionEnum.sqrt, new StandardSQLFunction("sqrt"));
        this.registerFunction(MathFunctionEnum.trunc, new StandardSQLFunction("trunc"));
        this.registerFunction(MathFunctionEnum.random, new StandardSQLFunction("rand"));
        this.registerFunction(MathFunctionEnum.radians, new StandardSQLFunction("radians"));
        this.registerFunction(MathFunctionEnum.acos, new StandardSQLFunction("acos"));
        this.registerFunction(MathFunctionEnum.asin, new StandardSQLFunction("asin"));
        this.registerFunction(MathFunctionEnum.atan, new StandardSQLFunction("atan"));
        this.registerFunction(MathFunctionEnum.atan2, new StandardSQLFunction("atan2"));
        this.registerFunction(MathFunctionEnum.cos, new StandardSQLFunction("cos"));
        this.registerFunction(MathFunctionEnum.sin, new StandardSQLFunction("sin"));
        this.registerFunction(MathFunctionEnum.tan, new StandardSQLFunction("tan"));

        this.registerFunction(MathFunctionEnum.bitand, new StandardSQLFunction("bitand"));

        // 日期函数
        // 不支持
        // 01 ADD_DAYS(date,n)
        // 02 ADD_MONTHS(date,n)
        // 03 ADD_WEEKS(date,n)
        // 09 DATEADD(datepart,n,date)
        // 10 DATEDIFF(datepart,date1,date2)
        // 11 DATEPART(datepart,date)
        // 12 DAY(date)
        // 13 DAYNAME(date)
        // 14 DAYOFMONTH(date)
        // 15 DAYOFWEEK(date)
        // 16 DAYOFYEAR(date)
        // 17 DAYS_BETWEEN(date1,date2)
        // 18 EXTRACT(时间字段 FROM date)
        // 20 GREATEST(date1,date2,date3)
        // 21 GREAT (date1,date2)
        // 22 HOUR(time)
        // 24 LEAST(date1, date2, date3)
        // 25 MINUTE(time)
        // 26 MONTH(date)
        // 27 MONTHNAME(date)
        // 28 MONTHS_BETWEEN(date1,date2)
        // 29 NEXT_DAY(date1,char2)
        // 31 QUARTER(date)
        // 32 SECOND(time)
        // 33 ROUND (date1[, fmt])
        // 34 TIMESTAMPADD(datepart,n,t imestamp)
        // 35 TIMESTAMPDIFF(datepart,ti meStamp1,timestamp2)
        // 37 TO_DATE(CHAR[,fmt[,'nls'] ]) /TO_TIMESTAMP(CHAR[,fmt[, 'nls']]) / TO_TIMESTAMP_TZ(CHAR[,fmt ])
        // 38 FROM_TZ(timestamp,timezon e|tz_name])
        // 39 TRUNC(date[,fmt])
        // 40 WEEK(date)
        // 41 WEEKDAY(date)
        // 42 WEEKS_BETWEEN(date1,date2)
        // 43 YEAR(date)
        // 44 YEARS_BETWEEN(date1,date2 )
        // 47 OVERLAPS
        // 48 TO_CHAR(date[,fmt[,nls]])
        // 50 NUMTODSINTERVAL(dec,inter val_unit)
        // 51 NUMTOYMNTERVAL(dec,interv al_unit)
        // 52 WEEK(date, mode)

        this.registerFunction(DateTypeFunctionEnum.to_char, new StandardSQLFunction("to_char"));
        this.registerFunction(DateTypeFunctionEnum.to_date, new StandardSQLFunction("to_date"));
        this.registerFunction(DateTypeFunctionEnum.to_number, new StandardSQLFunction("to_number"));
        this.registerFunction(DateTypeFunctionEnum.to_timestamp, new StandardSQLFunction("to_timestamp"));

        final SQLFunction currentDate = new NoArgumentSQLFunction("current_date", JdbcDataType.DATE);
        final SQLFunction currentTime = new NoArgumentSQLFunction("current_time(6)", JdbcDataType.TIME, false);
        final SQLFunction currentTimestamp = new NoArgumentSQLFunction("current_timestamp", JdbcDataType.TIMESTAMPTZ);
        final SQLFunction now = new NoArgumentSQLFunction("now(6)", JdbcDataType.TIMESTAMP, false);

        this.registerFunction(DateTypeFunctionEnum.clock_timestamp, now);    // Current date and time (changes during statement execution);

        // CURRENT_DATE and CURRENT_TIMESTAMP return the current date and time in the session time zone.
        this.registerFunction(DateTypeFunctionEnum.current_date,
                currentDate,
                new NoArgumentSQLFunction("curdate", JdbcDataType.DATE),
                new NoArgumentSQLFunction("getdate", JdbcDataType.DATETIME));
        this.registerFunction(DateTypeFunctionEnum.current_time,
                currentTime,
                new NoArgumentSQLFunction("curtime(6)", JdbcDataType.TIME, false));
        this.registerFunction(DateTypeFunctionEnum.current_timestamp,
                currentTimestamp);

        this.registerFunction(DateTypeFunctionEnum.extract, new StandardSQLFunction("extract"));
        this.registerFunction(DateTypeFunctionEnum.localtime,
                new NoArgumentSQLFunction("localtime(6)", JdbcDataType.TIME, false),
                new NoArgumentSQLFunction("sysdate", JdbcDataType.DATETIME));
        this.registerFunction(DateTypeFunctionEnum.localtimestamp,
                new NoArgumentSQLFunction("localtimestamp(6)", JdbcDataType.TIMESTAMP, false),
                new NoArgumentSQLFunction("systimestamp(6)", JdbcDataType.TIMESTAMP, false)
        );
        this.registerFunction(DateTypeFunctionEnum.now, now);    // Current date and time (start of current transaction);

        this.registerFunction(DateTypeFunctionEnum.statement_timestamp, now);    // Current date and time (start of current statement);
        this.registerFunction(DateTypeFunctionEnum.transaction_timestamp, now);  // Current date and time (start of current transaction);

        // 最后一天
        this.registerFunction(DateTypeFunctionEnum.last_day, new StandardSQLFunction("last_day"));

        // 带时区
        this.registerFunction(DateTypeFunctionEnum.datetime_offset, currentTimestamp);

        // utc类型
        this.registerFunction(DateTypeFunctionEnum.utc_datetime, new NoArgumentSQLFunction("getutcdate", JdbcDataType.TIMESTAMP));
        this.registerFunction(DateTypeFunctionEnum.utc_date, new NoArgumentSQLFunction("getutcdate", JdbcDataType.TIMESTAMP));
        this.registerFunction(DateTypeFunctionEnum.utc_time, new NoArgumentSQLFunction("getutcdate", JdbcDataType.TIMESTAMP));

        // 字符函数
        // 不支持
        // 02 ASCIISTR(char)
        // 08 DIFFERENCE(char1,char2)
        // 10 INS(char1,begin,n,char2)
        // 11 INSERT(char1,n1,n2,char2) / INSSTR(char1,n1,n2,char2)
        // 12 INSTR(char1,char2[,n,[m]] )
        // 13 INSTRB(char1,char2[,n,[m] ])
        // 19 COPYB(DEST_LOB,SRC_LOB,LEN[,DOFFSET[,SOFFSET]])
        // 32 SOUNDEX(char)
        // 33 SPACE(n)
        // 34 STRPOSDEC(char)
        // 35 STRPOSDEC(char,pos)
        // 36 STRPOSINC(char)
        // 37 STRPOSINC(char,pos)
        // 41 TO_CHAR(character)
        // 46 REGEXP
        // 48 TEXT_EQUAL
        // 49 BLOB_EQUAL
        // 50 NLSSORT(str1 [,nls_sort=str2])
        // 51 GREATEST(char1, char2, char3)
        // 52 GREAT (char1, char2)
        // 53 TO_SINGLE_BYTE (char)
        // 54 TO_MULTI_BYTE (char)
        // 55 EMPTY_CLOB ()
        // 56 EMPTY_BLOB ()
        // 57 UNISTR (char)

        this.registerFunction(StringFunctionEnum.bit_length, new StandardSQLFunction("bit_length"));
        this.registerFunction(StringFunctionEnum.char_length, new StandardSQLFunction("char_length"));
        this.registerFunction(StringFunctionEnum.character_length, new StandardSQLFunction("character_length"));
        this.registerFunction(StringFunctionEnum.lower,
                new StandardSQLFunction("lcase"),
                new StandardSQLFunction("lower"));
        this.registerFunction(StringFunctionEnum.octet_length, new StandardSQLFunction("octet_length"));
        this.registerFunction(StringFunctionEnum.overlay, new StandardSQLFunction("overlay"));
        this.registerFunction(StringFunctionEnum.locate, new StandardSQLFunction("locate"));
        this.registerFunction(StringFunctionEnum.position, new StandardSQLFunction("position"));
        this.registerFunction(StringFunctionEnum.substring,
                new StandardSQLFunction("substr"),
                new StandardSQLFunction("substrb"),
                new StandardSQLFunction("substring"));
        this.registerFunction(StringFunctionEnum.trim, new StandardSQLFunction("trim"));
        this.registerFunction(StringFunctionEnum.upper,
                new StandardSQLFunction("ucase"),
                new StandardSQLFunction("UPPER"));
        this.registerFunction(StringFunctionEnum.ascii, new StandardSQLFunction("ascii"));
        this.registerFunction(StringFunctionEnum.chr, new StandardSQLFunction("char"));
        this.registerFunction(StringFunctionEnum.concat, new StandardSQLFunction("concat"));
        this.registerFunction(StringFunctionEnum.initcap, new StandardSQLFunction("initcap"));
        this.registerFunction(StringFunctionEnum.left,
                new StandardSQLFunction("left"),
                new StandardSQLFunction("leftstr"));
        this.registerFunction(StringFunctionEnum.length,
                new StandardSQLFunction("len"),
                new StandardSQLFunction("length"));
        this.registerFunction(StringFunctionEnum.lpad, new StandardSQLFunction("lpad"));
        this.registerFunction(StringFunctionEnum.ltrim, new StandardSQLFunction("ltrim"));
        this.registerFunction(StringFunctionEnum.repeat,
                new StandardSQLFunction("repeat"),
                new StandardSQLFunction("repeatstr"),
                new StandardSQLFunction("replicate"));
        this.registerFunction(StringFunctionEnum.replace, new StandardSQLFunction("replace"));
        this.registerFunction(StringFunctionEnum.reverse, new StandardSQLFunction("reverse"));
        this.registerFunction(StringFunctionEnum.right,
                new StandardSQLFunction("right"),
                new StandardSQLFunction("rightstr"));
        this.registerFunction(StringFunctionEnum.rpad, new StandardSQLFunction("rpad"));
        this.registerFunction(StringFunctionEnum.rtrim, new StandardSQLFunction("rtrim"));
        this.registerFunction(StringFunctionEnum.translate, new StandardSQLFunction("translate"));

    }

    public void setLengthInChar(int lengthInChar) {
        this.lengthInChar = lengthInChar;
        // 默认为字节，需要转换为字符。
        if (lengthInChar == 0) {
            // 最长为4，如特殊字：𠮷
            this.charMaxLength = 4;
        }
    }

    @Override
    public char openQuote() {
        return '"';
    }

    @Override
    public char closeQuote() {
        return '"';
    }

    @Override
    public String toLookupName(String src) {
        return super.toLookupName(src.toUpperCase());
    }

    @Override
    public boolean supportsCommentOn() {
        return true;
    }

    @Override
    public String getTableCommentString(String tableName, String tableComment) {
        return String.format("comment on table %s is '%s'", tableName, tableComment);
    }

    @Override
    public String getColumnCommentString(String tableName, String columnName, String columnComment) {
        return String.format("comment on column %s.%s is '%s'", tableName, columnName, columnComment);
    }

    @Override
    public boolean supportsDropForeignKey() {
        return true;
    }

    @Override
    public boolean supportsPartitionBy() {
        return true;
    }

    @Override
    public boolean supportsSequences() {
        return true;
    }

    @Override
    public boolean supportsIdentityColumns() {
        return true;
    }

    @Override
    public boolean supportsCascadeDelete() {
        return true;
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
    public String getIdentityColumnString() {
        return "identity";
    }

    @Override
    public boolean supportsDisableIdentity() {
        // 《DM8 SQL语言使用手册.pdf》
        // 5.6 DM 自增列的使用
        return true;
    }

    @Override
    public String getDisableIdentityString(String tableName) {
        // 《DM8 SQL语言使用手册.pdf》
        // 5.6.2 SET IDENTITY_INSERT 属性
        return String.format("SET IDENTITY_INSERT %s ON", this.toLookupName(tableName));
    }

    @Override
    public String getEnableIdentityString(String tableName) {
        // 《DM8 SQL语言使用手册.pdf》
        // 5.6.2 SET IDENTITY_INSERT 属性
        return String.format("SET IDENTITY_INSERT %s OFF", this.toLookupName(tableName));
    }

    @Override
    public int getMaximumPrecision(int jdbcDataType) {
        switch (jdbcDataType) {
            case JdbcDataType.NUMERIC:
            case JdbcDataType.DECIMAL:
                return 38;
            case JdbcDataType.TIME:
            case JdbcDataType.TIMESTAMP:
            case JdbcDataType.TIMESTAMPTZ:
            case JdbcDataType.TIMESTAMPLTZ:
                return 6;
            case JdbcDataType.CHAR:
            case JdbcDataType.NCHAR:
            case JdbcDataType.BINARY:
                // 4K     1900
                // 8K     3900
                // 16K     8000
                // 32K     8188
                return 3900;
            case JdbcDataType.VARCHAR:
            case JdbcDataType.VARBINARY:
                return 8188;
            case JdbcDataType.NVARCHAR:
            case JdbcDataType.BIT:
                return 1;
            default:
                return 0;
        }
    }

    @Override
    public int getMinimumPrecision(int jdbcDataType) {
        switch (jdbcDataType) {
            case JdbcDataType.NUMERIC:
            case JdbcDataType.DECIMAL:
            case JdbcDataType.VARCHAR:
            case JdbcDataType.VARBINARY:
            case JdbcDataType.NVARCHAR:
            case JdbcDataType.BIT:
                return 1;
            case JdbcDataType.CHAR:
            case JdbcDataType.NCHAR:
            case JdbcDataType.BINARY:
                // 4K     1900
                // 8K     3900
                // 16K     8000
                // 32K     8188
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
        return 0;
    }

    @Override
    public int getRightDataTypeForVarchar(int columnSize, boolean[] doesIgnoredLength) {
        int maximumPrecision = this.getMaximumPrecision(JdbcDataType.VARCHAR);
        if (columnSize > maximumPrecision) {
            doesIgnoredLength[0] = true;
            return JdbcDataType.LONGVARCHAR;
        }
        return JdbcDataType.VARCHAR;
    }

    @Override
    public int getRightDataTypeForNVarchar(int columnSize, boolean[] doesIgnoredLength) {
        int maximumPrecision = this.getMaximumPrecision(JdbcDataType.NVARCHAR);
        if (columnSize > maximumPrecision) {
            doesIgnoredLength[0] = true;
            return JdbcDataType.LONGNVARCHAR;
        }
        return JdbcDataType.NVARCHAR;
    }

    @Override
    public int getRightDataTypeForChar(int columnSize, boolean[] doesIgnoredLength) {
        int maximumPrecision = this.getMaximumPrecision(JdbcDataType.CHAR);
        if (columnSize > maximumPrecision) {
            return this.getRightDataTypeForVarchar(columnSize, doesIgnoredLength);
        }
        return JdbcDataType.CHAR;
    }

    @Override
    public int getRightDataTypeForNChar(int columnSize, boolean[] doesIgnoredLength) {
        int maximumPrecision = this.getMaximumPrecision(JdbcDataType.NCHAR);
        if (columnSize > maximumPrecision) {
            return this.getRightDataTypeForNVarchar(columnSize, doesIgnoredLength);
        }
        return JdbcDataType.NCHAR;
    }

    @Override
    public int getRightDataTypeForBinary(int columnSize, boolean[] doesIgnoredLength) {
        int maximumPrecision = this.getMaximumPrecision(JdbcDataType.BINARY);
        if (columnSize > maximumPrecision) {
            return this.getRightDataTypeForVarBinary(columnSize, doesIgnoredLength);
        }
        return JdbcDataType.BINARY;
    }

    @Override
    public int getRightDataTypeForVarBinary(int columnSize, boolean[] doesIgnoredLength) {
        int maximumPrecision = this.getMaximumPrecision(JdbcDataType.VARBINARY);
        if (columnSize > maximumPrecision) {
            doesIgnoredLength[0] = true;
            return JdbcDataType.LONGVARBINARY;
        }
        return JdbcDataType.VARBINARY;
    }

    @Override
    public boolean supportsDecimalToInt() {
        return true;
    }

    @Override
    public boolean supportsYearToDate() {
        return true;
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
        return "CONTEXT";
    }

    @Override
    public void registerColumnTypeForYear(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForYear(ct, ci);
    }

    @Override
    public void registerColumnTypeForJson(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForJson(ct, ci);
    }

    @Override
    public void registerColumnTypeForSet(ColumnType ct, ColumnInfo ci) {
        ct.setLength(ci.getColumnSize() * charMaxLength);
    }

    @Override
    public void registerColumnTypeForEnum(ColumnType ct, ColumnInfo ci) {
        ct.setLength(ci.getColumnSize() * charMaxLength);
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
        super.registerColumnTypeForLongNVarchar(ct, ci);
    }

    @Override
    public void registerColumnTypeForNVarchar(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForNVarchar(ct, ci);
        ct.setLength(ci.getColumnSize() * charMaxLength);
    }

    @Override
    public void registerColumnTypeForNChar(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForNChar(ct, ci);
        ct.setLength(ci.getColumnSize() * charMaxLength);
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
        super.registerColumnTypeForLongVarbinary(ct, ci);
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
        super.registerColumnTypeForDate(ct, ci);
    }

    @Override
    public void registerColumnTypeForLongVarchar(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForLongVarchar(ct, ci);
    }

    @Override
    public void registerColumnTypeForVarchar(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForVarchar(ct, ci);
        ct.setLength(ci.getColumnSize() * charMaxLength);
    }

    @Override
    public void registerColumnTypeForChar(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForChar(ct, ci);
        ct.setLength(ci.getColumnSize() * charMaxLength);
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
        if (ci.isAutoincrement()) {
            // 自动增长必须为int以上的类型
            ct.setJdbcDataType(JdbcDataType.INTEGER);
        }
    }

    @Override
    public void registerColumnTypeForTinyint(ColumnType ct, ColumnInfo ci) {
        if (ci.isAutoincrement()) {
            // 自动增长必须为int以上的类型
            ct.setJdbcDataType(JdbcDataType.INTEGER);
        }
    }

    @Override
    public void registerColumnTypeForBit(ColumnType ct, ColumnInfo ci) {
        ct.setLength(-1);
    }

    @Override
    public boolean defaultUpperCaseName() {
        return true;
    }
}
