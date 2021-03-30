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
import org.springframework.stereotype.Service;

/**
 * @author baikai.liao
 * @Time 2021-03-14 10:08:47
 */
@Service
public class PgDatabaseDialect extends BasicDatabaseDialect {

    public PgDatabaseDialect() {

        // http://www.postgres.cn/docs/11/datatype.html

        this.registerColumnType(JdbcDataType.BIT, "bool");
        this.registerColumnType(JdbcDataType.BOOLEAN, "bool");

        this.registerColumnType(JdbcDataType.BINARY, "bytea");
        this.registerColumnType(JdbcDataType.VARBINARY, "bytea");
        this.registerColumnType(JdbcDataType.LONGVARBINARY, "bytea");
        this.registerColumnType(JdbcDataType.BLOB, "bytea");

        this.registerColumnType(JdbcDataType.TINYINT, "int2");
        this.registerColumnType(JdbcDataType.SMALLINT, "int2");
        this.registerColumnType(JdbcDataType.INTEGER, "int4");
        this.registerColumnType(JdbcDataType.BIGINT, "int8");

        this.registerColumnType(JdbcDataType.FLOAT, "float");
        this.registerColumnType(JdbcDataType.REAL, "float");
        this.registerColumnType(JdbcDataType.DOUBLE, "float8");

        this.registerColumnType(JdbcDataType.DECIMAL, "numeric");
        this.registerColumnType(JdbcDataType.NUMERIC, "numeric");

        this.registerColumnType(JdbcDataType.MONEY, "money");
        this.registerColumnType(JdbcDataType.SMALLMONEY, "money");

        this.registerColumnType(JdbcDataType.CHAR, "char");
        this.registerColumnType(JdbcDataType.NCHAR, "char");
        this.registerColumnType(JdbcDataType.VARCHAR, "varchar");
        this.registerColumnType(JdbcDataType.NVARCHAR, "varchar");

        this.registerColumnType(JdbcDataType.LONGVARCHAR, "text");
        this.registerColumnType(JdbcDataType.LONGNVARCHAR, "text");
        this.registerColumnType(JdbcDataType.CLOB, "text");
        this.registerColumnType(JdbcDataType.NCLOB, "text");

        this.registerColumnType(JdbcDataType.GUID, "char");

        this.registerColumnType(JdbcDataType.TIME, "time");
        this.registerColumnType(JdbcDataType.DATE, "date");
        this.registerColumnType(JdbcDataType.TIMESTAMP, "timestamp");

        this.registerColumnType(JdbcDataType.TIME_WITH_TIMEZONE, "timetz");
        this.registerColumnType(JdbcDataType.TIMESTAMP_WITH_TIMEZONE, "timestamptz");

        this.registerColumnType(JdbcDataType.TIMESTAMPTZ, "timestamptz");
        this.registerColumnType(JdbcDataType.TIMESTAMPLTZ, "timestamptz");

        this.registerColumnType(JdbcDataType.DATETIME, "timestamp");
        this.registerColumnType(JdbcDataType.DATETIMEOFFSET, "timestamp");
        this.registerColumnType(JdbcDataType.SMALLDATETIME, "timestamp");

        this.registerColumnType(JdbcDataType.ENUM, "varchar");
        this.registerColumnType(JdbcDataType.SET, "varchar");
        this.registerColumnType(JdbcDataType.JSON, "text");
        this.registerColumnType(JdbcDataType.YEAR, "date");

        this.registerFunction(MathFunctionEnum.abs, new StandardSQLFunction(MathFunctionEnum.abs.toString()));
        this.registerFunction(MathFunctionEnum.cbrt, new StandardSQLFunction(MathFunctionEnum.cbrt.toString()));
        this.registerFunction(MathFunctionEnum.ceil, new StandardSQLFunction(MathFunctionEnum.ceil.toString()));
        this.registerFunction(MathFunctionEnum.ceiling, new StandardSQLFunction(MathFunctionEnum.ceiling.toString()));
        this.registerFunction(MathFunctionEnum.degrees, new StandardSQLFunction(MathFunctionEnum.degrees.toString()));
        this.registerFunction(MathFunctionEnum.div, new StandardSQLFunction(MathFunctionEnum.div.toString()));
        this.registerFunction(MathFunctionEnum.exp, new StandardSQLFunction(MathFunctionEnum.exp.toString()));
        this.registerFunction(MathFunctionEnum.factorial, new StandardSQLFunction(MathFunctionEnum.factorial.toString()));
        this.registerFunction(MathFunctionEnum.floor, new StandardSQLFunction(MathFunctionEnum.floor.toString()));
        this.registerFunction(MathFunctionEnum.gcd, new StandardSQLFunction(MathFunctionEnum.gcd.toString()));
        this.registerFunction(MathFunctionEnum.lcm, new StandardSQLFunction(MathFunctionEnum.lcm.toString()));
        this.registerFunction(MathFunctionEnum.ln, new StandardSQLFunction(MathFunctionEnum.ln.toString()));
        this.registerFunction(MathFunctionEnum.log, new StandardSQLFunction(MathFunctionEnum.log.toString()));
        this.registerFunction(MathFunctionEnum.log10, new StandardSQLFunction(MathFunctionEnum.log10.toString()));
        this.registerFunction(MathFunctionEnum.min_scale, new StandardSQLFunction(MathFunctionEnum.min_scale.toString()));
        this.registerFunction(MathFunctionEnum.mod, new StandardSQLFunction(MathFunctionEnum.mod.toString()));
        this.registerFunction(MathFunctionEnum.pi, new StandardSQLFunction(MathFunctionEnum.pi.toString()));
        this.registerFunction(MathFunctionEnum.power, new StandardSQLFunction(MathFunctionEnum.power.toString()));
        this.registerFunction(MathFunctionEnum.radians, new StandardSQLFunction(MathFunctionEnum.radians.toString()));
        this.registerFunction(MathFunctionEnum.round, new StandardSQLFunction(MathFunctionEnum.round.toString()));
        this.registerFunction(MathFunctionEnum.scale, new StandardSQLFunction(MathFunctionEnum.scale.toString()));
        this.registerFunction(MathFunctionEnum.sign, new StandardSQLFunction(MathFunctionEnum.sign.toString()));
        this.registerFunction(MathFunctionEnum.sqrt, new StandardSQLFunction(MathFunctionEnum.sqrt.toString()));
        this.registerFunction(MathFunctionEnum.trunc, new StandardSQLFunction(MathFunctionEnum.trunc.toString()));
        this.registerFunction(MathFunctionEnum.width_bucket, new StandardSQLFunction(MathFunctionEnum.width_bucket.toString()));
        this.registerFunction(MathFunctionEnum.random, new StandardSQLFunction(MathFunctionEnum.random.toString()));
        this.registerFunction(MathFunctionEnum.acos, new StandardSQLFunction(MathFunctionEnum.acos.toString()));
        this.registerFunction(MathFunctionEnum.acosd, new StandardSQLFunction(MathFunctionEnum.acosd.toString()));
        this.registerFunction(MathFunctionEnum.asin, new StandardSQLFunction(MathFunctionEnum.asin.toString()));
        this.registerFunction(MathFunctionEnum.asind, new StandardSQLFunction(MathFunctionEnum.asind.toString()));
        this.registerFunction(MathFunctionEnum.atan, new StandardSQLFunction(MathFunctionEnum.atan.toString()));
        this.registerFunction(MathFunctionEnum.atand, new StandardSQLFunction(MathFunctionEnum.atand.toString()));
        this.registerFunction(MathFunctionEnum.atan2, new StandardSQLFunction(MathFunctionEnum.atan2.toString()));
        this.registerFunction(MathFunctionEnum.atan2d, new StandardSQLFunction(MathFunctionEnum.atan2d.toString()));
        this.registerFunction(MathFunctionEnum.cos, new StandardSQLFunction(MathFunctionEnum.cos.toString()));
        this.registerFunction(MathFunctionEnum.cosd, new StandardSQLFunction(MathFunctionEnum.cosd.toString()));
        this.registerFunction(MathFunctionEnum.cot, new StandardSQLFunction(MathFunctionEnum.cot.toString()));
        this.registerFunction(MathFunctionEnum.cotd, new StandardSQLFunction(MathFunctionEnum.cotd.toString()));
        this.registerFunction(MathFunctionEnum.sin, new StandardSQLFunction(MathFunctionEnum.sin.toString()));
        this.registerFunction(MathFunctionEnum.sind, new StandardSQLFunction(MathFunctionEnum.sind.toString()));
        this.registerFunction(MathFunctionEnum.tan, new StandardSQLFunction(MathFunctionEnum.tan.toString()));
        this.registerFunction(MathFunctionEnum.tand, new StandardSQLFunction(MathFunctionEnum.tand.toString()));

        // 时间函数
        this.registerFunction(DateTypeFunctionEnum.to_char, new StandardSQLFunction(DateTypeFunctionEnum.to_char.toString()));
        this.registerFunction(DateTypeFunctionEnum.to_date, new StandardSQLFunction(DateTypeFunctionEnum.to_date.toString()));
        this.registerFunction(DateTypeFunctionEnum.to_number, new StandardSQLFunction(DateTypeFunctionEnum.to_number.toString()));
        this.registerFunction(DateTypeFunctionEnum.to_timestamp, new StandardSQLFunction(DateTypeFunctionEnum.to_timestamp.toString()));

        this.registerFunction(DateTypeFunctionEnum.age, new StandardSQLFunction(DateTypeFunctionEnum.age.toString()));
        this.registerFunction(DateTypeFunctionEnum.clock_timestamp, new StandardSQLFunction(DateTypeFunctionEnum.clock_timestamp.toString()));
        this.registerFunction(DateTypeFunctionEnum.current_date, new StandardSQLFunction(DateTypeFunctionEnum.current_date.toString()));
        this.registerFunction(DateTypeFunctionEnum.current_time, new StandardSQLFunction(DateTypeFunctionEnum.current_time.toString()));
        this.registerFunction(DateTypeFunctionEnum.current_timestamp, new StandardSQLFunction(DateTypeFunctionEnum.current_timestamp.toString()));
        this.registerFunction(DateTypeFunctionEnum.date_part, new StandardSQLFunction(DateTypeFunctionEnum.date_part.toString()));
        this.registerFunction(DateTypeFunctionEnum.date_trunc, new StandardSQLFunction(DateTypeFunctionEnum.date_trunc.toString()));
        this.registerFunction(DateTypeFunctionEnum.extract, new StandardSQLFunction(DateTypeFunctionEnum.extract.toString()));
        this.registerFunction(DateTypeFunctionEnum.isfinite, new StandardSQLFunction(DateTypeFunctionEnum.isfinite.toString()));
        this.registerFunction(DateTypeFunctionEnum.justify_days, new StandardSQLFunction(DateTypeFunctionEnum.justify_days.toString()));
        this.registerFunction(DateTypeFunctionEnum.justify_hours, new StandardSQLFunction(DateTypeFunctionEnum.justify_hours.toString()));
        this.registerFunction(DateTypeFunctionEnum.justify_interval, new StandardSQLFunction(DateTypeFunctionEnum.justify_interval.toString()));
        this.registerFunction(DateTypeFunctionEnum.localtime, new StandardSQLFunction(DateTypeFunctionEnum.localtime.toString()));
        this.registerFunction(DateTypeFunctionEnum.localtimestamp, new StandardSQLFunction(DateTypeFunctionEnum.localtimestamp.toString()));
        this.registerFunction(DateTypeFunctionEnum.make_date, new StandardSQLFunction(DateTypeFunctionEnum.make_date.toString()));
        this.registerFunction(DateTypeFunctionEnum.make_interval, new StandardSQLFunction(DateTypeFunctionEnum.make_interval.toString()));
        this.registerFunction(DateTypeFunctionEnum.make_time, new StandardSQLFunction(DateTypeFunctionEnum.make_time.toString()));
        this.registerFunction(DateTypeFunctionEnum.make_timestamp, new StandardSQLFunction(DateTypeFunctionEnum.make_timestamp.toString()));
        this.registerFunction(DateTypeFunctionEnum.make_timestamptz, new StandardSQLFunction(DateTypeFunctionEnum.make_timestamptz.toString()));
        this.registerFunction(DateTypeFunctionEnum.now, new StandardSQLFunction(DateTypeFunctionEnum.now.toString()));
        this.registerFunction(DateTypeFunctionEnum.statement_timestamp, new StandardSQLFunction(DateTypeFunctionEnum.statement_timestamp.toString()));
        this.registerFunction(DateTypeFunctionEnum.timeofday, new StandardSQLFunction(DateTypeFunctionEnum.timeofday.toString()));
        this.registerFunction(DateTypeFunctionEnum.transaction_timestamp, new StandardSQLFunction(DateTypeFunctionEnum.transaction_timestamp.toString()));

        // 带时区
        this.registerFunction(DateTypeFunctionEnum.datetime_offset, new NoArgumentSQLFunction("current_timestamp", JdbcDataType.TIMESTAMP, false));
        // utc类型
        this.registerFunction(DateTypeFunctionEnum.utc_datetime, new NoArgumentSQLFunction("current_timestamp at time zone 'utc'", JdbcDataType.TIMESTAMP, false));
        this.registerFunction(DateTypeFunctionEnum.utc_date, new NoArgumentSQLFunction("current_date at time zone 'utc'", JdbcDataType.DATE));
        this.registerFunction(DateTypeFunctionEnum.utc_time, new NoArgumentSQLFunction("current_time at time zone 'utc'", JdbcDataType.TIME));


        // 字符函数
        this.registerFunction(StringFunctionEnum.bit_length, new StandardSQLFunction(StringFunctionEnum.bit_length.toString()));
        this.registerFunction(StringFunctionEnum.char_length, new StandardSQLFunction(StringFunctionEnum.char_length.toString()));
        this.registerFunction(StringFunctionEnum.character_length, new StandardSQLFunction(StringFunctionEnum.character_length.toString()));
        this.registerFunction(StringFunctionEnum.lower, new StandardSQLFunction(StringFunctionEnum.lower.toString()));
        this.registerFunction(StringFunctionEnum.normalize, new StandardSQLFunction(StringFunctionEnum.normalize.toString()));
        this.registerFunction(StringFunctionEnum.octet_length, new StandardSQLFunction(StringFunctionEnum.octet_length.toString()));
        this.registerFunction(StringFunctionEnum.overlay, new StandardSQLFunction(StringFunctionEnum.overlay.toString()));
        this.registerFunction(StringFunctionEnum.position, new StandardSQLFunction(StringFunctionEnum.position.toString()));    // position(a in b)
        this.registerFunction(StringFunctionEnum.substring, new StandardSQLFunction(StringFunctionEnum.substring.toString()));
        this.registerFunction(StringFunctionEnum.trim, new StandardSQLFunction(StringFunctionEnum.trim.toString()));
        this.registerFunction(StringFunctionEnum.upper, new StandardSQLFunction(StringFunctionEnum.upper.toString()));
        this.registerFunction(StringFunctionEnum.ascii, new StandardSQLFunction(StringFunctionEnum.ascii.toString()));
        this.registerFunction(StringFunctionEnum.btrim, new StandardSQLFunction(StringFunctionEnum.btrim.toString()));
        this.registerFunction(StringFunctionEnum.chr, new StandardSQLFunction(StringFunctionEnum.chr.toString()));
        this.registerFunction(StringFunctionEnum.concat, new StandardSQLFunction(StringFunctionEnum.concat.toString()));
        this.registerFunction(StringFunctionEnum.concat_ws, new StandardSQLFunction(StringFunctionEnum.concat_ws.toString()));
        this.registerFunction(StringFunctionEnum.format, new StandardSQLFunction(StringFunctionEnum.format.toString()));
        this.registerFunction(StringFunctionEnum.initcap, new StandardSQLFunction(StringFunctionEnum.initcap.toString()));
        this.registerFunction(StringFunctionEnum.left, new StandardSQLFunction(StringFunctionEnum.left.toString()));
        this.registerFunction(StringFunctionEnum.length, new StandardSQLFunction(StringFunctionEnum.length.toString()));
        this.registerFunction(StringFunctionEnum.lpad, new StandardSQLFunction(StringFunctionEnum.lpad.toString()));
        this.registerFunction(StringFunctionEnum.ltrim, new StandardSQLFunction(StringFunctionEnum.ltrim.toString()));
        this.registerFunction(StringFunctionEnum.md5, new StandardSQLFunction(StringFunctionEnum.md5.toString()));
        this.registerFunction(StringFunctionEnum.parse_ident, new StandardSQLFunction(StringFunctionEnum.parse_ident.toString()));
        this.registerFunction(StringFunctionEnum.quote_ident, new StandardSQLFunction(StringFunctionEnum.quote_ident.toString()));
        this.registerFunction(StringFunctionEnum.quote_literal, new StandardSQLFunction(StringFunctionEnum.quote_literal.toString()));
        this.registerFunction(StringFunctionEnum.quote_nullable, new StandardSQLFunction(StringFunctionEnum.quote_nullable.toString()));
        this.registerFunction(StringFunctionEnum.regexp_match, new StandardSQLFunction(StringFunctionEnum.regexp_match.toString()));
        this.registerFunction(StringFunctionEnum.regexp_matches, new StandardSQLFunction(StringFunctionEnum.regexp_matches.toString()));
        this.registerFunction(StringFunctionEnum.regexp_replace, new StandardSQLFunction(StringFunctionEnum.regexp_replace.toString()));
        this.registerFunction(StringFunctionEnum.regexp_split_to_array, new StandardSQLFunction(StringFunctionEnum.regexp_split_to_array.toString()));
        this.registerFunction(StringFunctionEnum.regexp_split_to_table, new StandardSQLFunction(StringFunctionEnum.regexp_split_to_table.toString()));
        this.registerFunction(StringFunctionEnum.repeat, new StandardSQLFunction(StringFunctionEnum.repeat.toString()));
        this.registerFunction(StringFunctionEnum.replace, new StandardSQLFunction(StringFunctionEnum.replace.toString()));
        this.registerFunction(StringFunctionEnum.reverse, new StandardSQLFunction(StringFunctionEnum.reverse.toString()));
        this.registerFunction(StringFunctionEnum.right, new StandardSQLFunction(StringFunctionEnum.right.toString()));
        this.registerFunction(StringFunctionEnum.rpad, new StandardSQLFunction(StringFunctionEnum.rpad.toString()));
        this.registerFunction(StringFunctionEnum.rtrim, new StandardSQLFunction(StringFunctionEnum.rtrim.toString()));
        this.registerFunction(StringFunctionEnum.split_part, new StandardSQLFunction(StringFunctionEnum.split_part.toString()));
        this.registerFunction(StringFunctionEnum.strpos, new StandardSQLFunction(StringFunctionEnum.strpos.toString()));
        this.registerFunction(StringFunctionEnum.substr, new StandardSQLFunction(StringFunctionEnum.substr.toString()));
        this.registerFunction(StringFunctionEnum.starts_with, new StandardSQLFunction(StringFunctionEnum.starts_with.toString()));
        this.registerFunction(StringFunctionEnum.to_ascii, new StandardSQLFunction(StringFunctionEnum.to_ascii.toString()));
        this.registerFunction(StringFunctionEnum.to_hex, new StandardSQLFunction(StringFunctionEnum.to_hex.toString()));
        this.registerFunction(StringFunctionEnum.translate, new StandardSQLFunction(StringFunctionEnum.translate.toString()));
        this.registerFunction(StringFunctionEnum.convert, new StandardSQLFunction(StringFunctionEnum.convert.toString()));
        this.registerFunction(StringFunctionEnum.convert_from, new StandardSQLFunction(StringFunctionEnum.convert_from.toString()));
        this.registerFunction(StringFunctionEnum.convert_to, new StandardSQLFunction(StringFunctionEnum.convert_to.toString()));
        this.registerFunction(StringFunctionEnum.encode, new StandardSQLFunction(StringFunctionEnum.encode.toString()));
        this.registerFunction(StringFunctionEnum.decode, new StandardSQLFunction(StringFunctionEnum.decode.toString()));

        // ERROR:  function gen_random_uuid() does not exist
        // LINE 1: select gen_random_uuid();
        //                ^
        // HINT:  No function matches the given name and argument types. You might need to add explicit type casts.
        // CREATE EXTENSION pgcrypto;
//     postgres=# select gen_random_uuid();
//     gen_random_uuid
// --------------------------------------
//     b9babd56-decd-4503-85ed-5250da5c2530
        this.registerFunction(StringFunctionEnum.uuid, new NoArgumentSQLFunction("gen_random_uuid", JdbcDataType.VARCHAR));

    }

    @Override
    public char openQuote() {
        // postgres=# \d
        //          List of relations
        //  Schema |  Name  | Type  |  Owner
        // --------+--------+-------+----------
        //  public | TABLE2 | table | postgres
        //  public | table2 | table | postgres
        // 经测试发现，PostgreSQL使用双引号创建表的时候，会保留大小写。
        // 不用双引号的时候，默认会转成小写。
        return '"';
    }

    @Override
    public char closeQuote() {
        return '"';
    }

    @Override
    public boolean supportsCommentOn() {
        return true;
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
    public String toBooleanValueString(Object value, int jdbcDataType) {
        return null;
    }

    @Override
    public boolean supportsTruncateTable() {
        return true;
    }

    /**
     * {@link org.postgresql.jdbc.TypeInfoCache}
     *
     * @param jdbcDataType jdbc类型
     * @return
     */
    @Override
    public int getMaximumPrecision(int jdbcDataType) {
        switch (jdbcDataType) {
            case JdbcDataType.NUMERIC:
            case JdbcDataType.DECIMAL:
                return 1000;
            case JdbcDataType.TIME:
            case JdbcDataType.TIME_WITH_TIMEZONE:
                // Technically this depends on the --enable-integer-datetimes
                // configure setting. It is 6 with integer and 10 with float.
                return 6;
            case JdbcDataType.TIMESTAMP:
            case JdbcDataType.TIMESTAMPTZ:
                return 6;
            case JdbcDataType.CHAR:
            case JdbcDataType.VARCHAR:
                return 10485760;
            case JdbcDataType.BIT:
                return 83886080;
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
            case JdbcDataType.CHAR:
            case JdbcDataType.VARCHAR:
                return 1;
            case JdbcDataType.TIME:
            case JdbcDataType.TIME_WITH_TIMEZONE:
                // Technically this depends on the --enable-integer-datetimes
                // configure setting. It is 6 with integer and 10 with float.
                return 0;
            default:
                return 0;
        }
    }

    @Override
    public int getMaximumScale(int jdbcDataType) {
        switch (jdbcDataType) {
            case JdbcDataType.NUMERIC:
            case JdbcDataType.DECIMAL:
                return 1000;
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
        if (columnSize > this.getMaximumPrecision(JdbcDataType.VARCHAR)) {
            doesIgnoredLength[0] = true;
            return JdbcDataType.LONGVARCHAR;
        }

        return JdbcDataType.VARCHAR;
    }

    @Override
    public int getRightDataTypeForNVarchar(int columnSize, boolean[] doesIgnoredLength) {
        return getRightDataTypeForVarchar(columnSize, doesIgnoredLength);
    }

    @Override
    public int getRightDataTypeForChar(int columnSize, boolean[] doesIgnoredLength) {
        if (columnSize > this.getMaximumPrecision(JdbcDataType.CHAR)) {
            doesIgnoredLength[0] = true;
            return JdbcDataType.LONGVARCHAR;
        }
        return JdbcDataType.CHAR;
    }

    @Override
    public int getRightDataTypeForNChar(int columnSize, boolean[] doesIgnoredLength) {
        return this.getRightDataTypeForChar(columnSize, doesIgnoredLength);
    }

    @Override
    public int getRightDataTypeForBinary(int columnSize, boolean[] doesIgnoredLength) {
        return JdbcDataType.LONGVARBINARY;
    }

    @Override
    public int getRightDataTypeForVarBinary(int columnSize, boolean[] doesIgnoredLength) {
        return JdbcDataType.LONGVARBINARY;
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
        super.registerColumnTypeForSet(ct, ci);
    }

    @Override
    public void registerColumnTypeForEnum(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForEnum(ct, ci);
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
    public String getDropTableString(String tableName) {
        return super.getDropTableString(tableName);
    }

    @Override
    public void registerColumnTypeForBit(ColumnType ct, ColumnInfo ci) {
        ct.setLength(-1);
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
    public boolean supportsFullTextIndex() {
        return false;
    }

    @Override
    public String getFullTextIndexString() {
        return null;
    }

    @Override
    public boolean supportsSpatialIndex() {
        return true;
    }

    @Override
    public boolean defaultUpperCaseName() {
        return false;
    }

    @Override
    public boolean defaultLowerCaseName() {
        return true;
    }

    @Override
    public boolean supportsIdentityReplaceColumnType() {
        return true;
    }

    @Override
    public String getIdentityColumnString() {
        return "serial";    // int4
    }
}
