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
 * Microsoft Access数据库方言
 * References:
 * http://ucanaccess.sourceforge.net/site.html#home
 * <p>
 * Access data types: YESNO, BYTE, INTEGER, LONG, SINGLE, DOUBLE, NUMERIC, CURRENCY, COUNTER, TEXT, OLE, MEMO, GUID, DATETIME.
 *
 * @author baikai.liao
 * @Time 2021-03-12 17:28:09
 */
@Service
public class AccessDatabaseDialect extends BasicDatabaseDialect {

    public AccessDatabaseDialect() {

        this.registerColumnType(JdbcDataType.BIT, "YESNO");
        this.registerColumnType(JdbcDataType.BOOLEAN, "YESNO");

        this.registerColumnType(JdbcDataType.BINARY, "BYTE");
        this.registerColumnType(JdbcDataType.VARBINARY, "BYTE");
        this.registerColumnType(JdbcDataType.LONGVARBINARY, "OLE");
        this.registerColumnType(JdbcDataType.BLOB, "OLE");

        this.registerColumnType(JdbcDataType.TINYINT, "INTEGER");
        this.registerColumnType(JdbcDataType.SMALLINT, "INTEGER");
        this.registerColumnType(JdbcDataType.INTEGER, "INTEGER");
        this.registerColumnType(JdbcDataType.BIGINT, "LONG");

        this.registerColumnType(JdbcDataType.FLOAT, "SINGLE");
        this.registerColumnType(JdbcDataType.REAL, "SINGLE");
        this.registerColumnType(JdbcDataType.DOUBLE, "DOUBLE");

        this.registerColumnType(JdbcDataType.DECIMAL, "NUMERIC");
        this.registerColumnType(JdbcDataType.NUMERIC, "NUMERIC");

        this.registerColumnType(JdbcDataType.MONEY, "CURRENCY");
        this.registerColumnType(JdbcDataType.SMALLMONEY, "CURRENCY");

        this.registerColumnType(JdbcDataType.CHAR, "TEXT");
        this.registerColumnType(JdbcDataType.NCHAR, "TEXT");
        this.registerColumnType(JdbcDataType.VARCHAR, "TEXT");
        this.registerColumnType(JdbcDataType.NVARCHAR, "TEXT");

        this.registerColumnType(JdbcDataType.LONGVARCHAR, "MEMO");
        this.registerColumnType(JdbcDataType.LONGNVARCHAR, "MEMO");
        this.registerColumnType(JdbcDataType.CLOB, "MEMO");
        this.registerColumnType(JdbcDataType.NCLOB, "MEMO");

        this.registerColumnType(JdbcDataType.GUID, "GUID");

        this.registerColumnType(JdbcDataType.TIME, "DATETIME");
        this.registerColumnType(JdbcDataType.DATE, "DATETIME");
        this.registerColumnType(JdbcDataType.TIMESTAMP, "DATETIME");

        this.registerColumnType(JdbcDataType.TIME_WITH_TIMEZONE, "DATETIME");
        this.registerColumnType(JdbcDataType.TIMESTAMP_WITH_TIMEZONE, "DATETIME");

        this.registerColumnType(JdbcDataType.TIMESTAMPTZ, "DATETIME");
        this.registerColumnType(JdbcDataType.TIMESTAMPLTZ, "DATETIME");

        this.registerColumnType(JdbcDataType.DATETIME, "DATETIME");
        this.registerColumnType(JdbcDataType.DATETIMEOFFSET, "DATETIME");
        this.registerColumnType(JdbcDataType.SMALLDATETIME, "DATETIME");

        this.registerColumnType(JdbcDataType.ENUM, "TEXT");
        this.registerColumnType(JdbcDataType.SET, "TEXT");
        this.registerColumnType(JdbcDataType.JSON, "MEMO");
        this.registerColumnType(JdbcDataType.YEAR, "DATETIME");

        // http://ucanaccess.sourceforge.net/site.html#examples
        // ASC, ATN, CBOOL, CCUR, CDATE, CDBL, CDEC, CINT, CLONG, CSIGN, CSTR, CVAR, DATEADD, DATEDIFF, DATEPART, DATE, DATESERIAL, DATEVALUE, FIX, FORMAT, IIF, INSTR, INSTRREV, ISDATE, ISNUMERIC, INT, IsNull, LEN, MID, MONTHNAME, NOW, NZ, PARTITION, SIGN, SPACE, SQR, STR,, STRING, STRCOMP, STRCONV, STRREVERSE, SWITCH, RND, TIME, TIMESERIAL, VAL, WEEKDAY, WEEKDAYNAME;
        // Aggregate and Domain Functions: FIRST, LAST, DCOUNT, DAVG, DSUM, DMAX, DMIN, DFIRST, DLAST, DLOOKUP.
        // Financial Functions(since UCanAccess 2.0.7.1): PMT, NPER, IPMT, PPMT, RATE, PV, FV, DDB, SYD, SLN.
        // Also you can use the following functions from the hsqldb implementation:
        // COS, SIN, LTRIM, RTRIM, UCASE, LCASE;
        // Aggregate Functions: COUNT, AVG, SUM, MAX, MIN, STDEV, STDEVP, VAR, VARP.

        /**
         * {@link net.ucanaccess.converters.Functions }
         */
        // this.registerFunction(MathFunctionEnum.abs, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.cbrt, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.ceil, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.ceiling, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.degrees, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.div, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.exp, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.factorial, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.floor, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.gcd, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.lcm, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.ln, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.log, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.log10, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.min_scale, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.mod, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.pi, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.power, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.radians, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.round, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.scale, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.sign, new StandardSQLFunction("sign"));
        // this.registerFunction(MathFunctionEnum.sqrt, new StandardSQLFunction("sqr"));
        // this.registerFunction(MathFunctionEnum.trunc, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.width_bucket, new StandardSQLFunction(""));
        this.registerFunction(MathFunctionEnum.random, new NoArgumentSQLFunction("rnd", JdbcDataType.DOUBLE));
        // this.registerFunction(MathFunctionEnum.acos, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.acosd, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.asin, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.asind, new StandardSQLFunction(""));
        this.registerFunction(MathFunctionEnum.atan, new StandardSQLFunction("atn"));
        // this.registerFunction(MathFunctionEnum.atand, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.atan2, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.atan2d, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.cos, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.cosd, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.cot, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.cotd, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.sin, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.sind, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.tan, new StandardSQLFunction(""));
        // this.registerFunction(MathFunctionEnum.tand, new StandardSQLFunction(""));


        // this.registerFunction(DateTypeFunctionEnum.to_char, new StandardSQLFunction(""));
        this.registerFunction(DateTypeFunctionEnum.to_date, new StandardSQLFunction("cdate"));
        // this.registerFunction(DateTypeFunctionEnum.to_number, new StandardSQLFunction(""));
        // this.registerFunction(DateTypeFunctionEnum.to_timestamp, new StandardSQLFunction(""));

        // http://ucanaccess.sourceforge.net/site.html#examples
        final SQLFunction now = new NoArgumentSQLFunction("now", JdbcDataType.TIMESTAMP);
        final SQLFunction time = new NoArgumentSQLFunction("time", JdbcDataType.TIME);
        this.registerFunction(DateTypeFunctionEnum.age, new StandardSQLFunction("datediff"));
        this.registerFunction(DateTypeFunctionEnum.clock_timestamp, now);    // Current date and time (changes during statement execution);
        this.registerFunction(DateTypeFunctionEnum.current_date, now);
        this.registerFunction(DateTypeFunctionEnum.current_time, time);
        this.registerFunction(DateTypeFunctionEnum.current_timestamp, now);
        this.registerFunction(DateTypeFunctionEnum.date_part, new StandardSQLFunction("datepart"));
        // this.registerFunction(DateTypeFunctionEnum.date_trunc, new StandardSQLFunction(""));
        // this.registerFunction(DateTypeFunctionEnum.extract, new StandardSQLFunction(""));
        // this.registerFunction(DateTypeFunctionEnum.isfinite, new StandardSQLFunction(""));
        // this.registerFunction(DateTypeFunctionEnum.justify_days, new StandardSQLFunction(""));
        // this.registerFunction(DateTypeFunctionEnum.justify_hours, new StandardSQLFunction(""));
        // this.registerFunction(DateTypeFunctionEnum.justify_interval, new StandardSQLFunction(""));
        this.registerFunction(DateTypeFunctionEnum.localtime, time);
        this.registerFunction(DateTypeFunctionEnum.localtimestamp, now);
        // this.registerFunction(DateTypeFunctionEnum.make_date, new StandardSQLFunction(""));
        // this.registerFunction(DateTypeFunctionEnum.make_interval, new StandardSQLFunction(""));
        // this.registerFunction(DateTypeFunctionEnum.make_time, new StandardSQLFunction(""));
        // this.registerFunction(DateTypeFunctionEnum.make_timestamp, new StandardSQLFunction(""));
        // this.registerFunction(DateTypeFunctionEnum.make_timestamptz, new StandardSQLFunction(""));
        this.registerFunction(DateTypeFunctionEnum.now, now);    // Current date and time (start of current transaction);
        this.registerFunction(DateTypeFunctionEnum.statement_timestamp, now);    // Current date and time (start of current statement);
        this.registerFunction(DateTypeFunctionEnum.timeofday, time);
        this.registerFunction(DateTypeFunctionEnum.transaction_timestamp, now);  // Current date and time (start of current transaction);

        this.registerFunction(DateTypeFunctionEnum.datetime_offset, now);
        this.registerFunction(DateTypeFunctionEnum.utc_datetime, now);
        this.registerFunction(DateTypeFunctionEnum.utc_date, now);
        this.registerFunction(DateTypeFunctionEnum.utc_time, time);

        // this.registerFunction(StringFunctionEnum.bit_length, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.char_length, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.character_length, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.lower, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.normalize, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.octet_length, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.overlay, new StandardSQLFunction(""));
        this.registerFunction(StringFunctionEnum.instr, new StandardSQLFunction("instr"));
        this.registerFunction(StringFunctionEnum.position, new StandardSQLFunction("instr"));
        // this.registerFunction(StringFunctionEnum.substring, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.trim, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.upper, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.ascii, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.btrim, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.chr, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.concat, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.concat_ws, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.format, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.initcap, new StandardSQLFunction(""));
        this.registerFunction(StringFunctionEnum.left, new StandardSQLFunction("left"));
        this.registerFunction(StringFunctionEnum.length, new StandardSQLFunction("len"));
        // this.registerFunction(StringFunctionEnum.lpad, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.ltrim, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.md5, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.parse_ident, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.quote_ident, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.quote_literal, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.quote_nullable, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.regexp_match, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.regexp_matches, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.regexp_replace, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.regexp_split_to_array, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.regexp_split_to_table, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.repeat, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.replace, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.reverse, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.right, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.rpad, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.rtrim, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.split_part, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.strpos, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.substr, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.starts_with, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.to_ascii, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.to_hex, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.translate, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.convert, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.convert_from, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.convert_to, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.encode, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.decode, new StandardSQLFunction(""));
        // this.registerFunction(StringFunctionEnum.uuid, new StandardSQLFunction(""));

    }

    @Override
    public char openQuote() {
        return '[';
    }

    @Override
    public char closeQuote() {
        return ']';
    }

    public boolean supportsDropTable() {
        return false;
    }

    @Override
    public boolean supportsIdentityReplaceColumnType() {
        return true;
    }

    @Override
    public boolean supportsIdentityColumns() {
        return true;
    }

    @Override
    public String getIdentityColumnString() {
        return "autoincrement";
    }

    @Override
    public boolean supportsDisableIdentity() {
        return true;
    }

    @Override
    public String getDisableIdentityString(String tableName) {
        return "DISABLE AUTOINCREMENT ON " + this.toLookupName(tableName);
    }

    @Override
    public String getEnableIdentityString(String tableName) {
        return "ENABLE AUTOINCREMENT ON " + this.toLookupName(tableName);
    }

    @Override
    public boolean supportsLimit() {
        return true;
    }

    @Override
    public String toBooleanValueString(Object value, int jdbcDataType) {
        if (value == null) {
            return null;
        }

        if (value instanceof Boolean) {
            return String.valueOf(value);
        } else if (value instanceof String) {
            return (String) value;
        }
        return value.toString();
    }

    @Override
    public boolean supportsTruncateTable() {
        return false;
    }

    @Override
    public int getMaximumPrecision(int jdbcDataType) {
        switch (jdbcDataType) {
            case JdbcDataType.NUMERIC:
            case JdbcDataType.DECIMAL:
                return 28;
            case JdbcDataType.TIME:
            case JdbcDataType.TIMESTAMP:
            case JdbcDataType.TIMESTAMPTZ:
                return 6;
            case JdbcDataType.CHAR:
            case JdbcDataType.NCHAR:
            case JdbcDataType.VARCHAR:
            case JdbcDataType.NVARCHAR:
            case JdbcDataType.BINARY:
            case JdbcDataType.VARBINARY:
                return 0xFF;
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
            case JdbcDataType.BIT:
                return 1;
            case JdbcDataType.VARCHAR:
                return 0xFF;
            default:
                return 0;
        }
    }

    @Override
    public int getMaximumScale(int jdbcDataType) {
        switch (jdbcDataType) {
            case JdbcDataType.NUMERIC:
            case JdbcDataType.DECIMAL:
                return 28;
            default:
                return 0;
        }
    }

    @Override
    public int getMinimumScale(int jdbcDataType) {
        switch (jdbcDataType) {
            case JdbcDataType.NUMERIC:
            case JdbcDataType.DECIMAL:
                return 1;
            default:
                return 0;
        }
    }

    @Override
    public int getRightDataTypeForVarchar(int columnSize, boolean[] doesIgnoredLength) {
        doesIgnoredLength[0] = true;
        int maximumPrecision = this.getMaximumPrecision(JdbcDataType.VARCHAR);
        // MEMO, TEXT
        return columnSize > maximumPrecision ? JdbcDataType.LONGVARCHAR : JdbcDataType.VARCHAR;
    }

    @Override
    public int getRightDataTypeForNVarchar(int columnSize, boolean[] doesIgnoredLength) {
        doesIgnoredLength[0] = true;
        return this.getRightDataTypeForVarchar(columnSize, doesIgnoredLength);
    }

    @Override
    public int getRightDataTypeForChar(int columnSize, boolean[] doesIgnoredLength) {
        doesIgnoredLength[0] = true;
        // 无此类型
        return this.getRightDataTypeForVarchar(columnSize, doesIgnoredLength);
    }

    @Override
    public int getRightDataTypeForNChar(int columnSize, boolean[] doesIgnoredLength) {
        doesIgnoredLength[0] = true;
        // 无此类型
        return this.getRightDataTypeForVarchar(columnSize, doesIgnoredLength);
    }

    @Override
    public int getRightDataTypeForBinary(int columnSize, boolean[] doesIgnoredLength) {
        doesIgnoredLength[0] = true;
        int maximumPrecision = this.getMaximumPrecision(JdbcDataType.BINARY);
        // OLE, BYTE
        return columnSize > maximumPrecision ? JdbcDataType.LONGVARBINARY : JdbcDataType.BINARY;
    }

    @Override
    public int getRightDataTypeForVarBinary(int columnSize, boolean[] doesIgnoredLength) {
        doesIgnoredLength[0] = true;
        return this.getRightDataTypeForBinary(columnSize, doesIgnoredLength);
    }

    @Override
    public boolean supportsYearToDate() {
        return true;
    }

    @Override
    public String getFullTextIndexString() {
        return null;
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
    }

    @Override
    public void registerColumnTypeForChar(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForChar(ct, ci);
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
        ct.setLength(-1);
    }
}
