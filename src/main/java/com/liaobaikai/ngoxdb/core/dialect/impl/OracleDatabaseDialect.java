package com.liaobaikai.ngoxdb.core.dialect.impl;

import com.liaobaikai.ngoxdb.bean.ColumnType;
import com.liaobaikai.ngoxdb.bean.PreparedPagination;
import com.liaobaikai.ngoxdb.bean.info.ColumnInfo;
import com.liaobaikai.ngoxdb.core.constant.JdbcDataType;
import com.liaobaikai.ngoxdb.core.dialect.BasicDatabaseDialect;
import com.liaobaikai.ngoxdb.core.dialect.DatabaseDialect;
import com.liaobaikai.ngoxdb.core.enums.func.DateTypeFunctionEnum;
import com.liaobaikai.ngoxdb.core.enums.func.MathFunctionEnum;
import com.liaobaikai.ngoxdb.core.enums.func.StringFunctionEnum;
import com.liaobaikai.ngoxdb.core.func.SQLFunction;
import com.liaobaikai.ngoxdb.core.func.impl.DynamicSQLFunction;
import com.liaobaikai.ngoxdb.core.func.impl.NoArgumentSQLFunction;
import com.liaobaikai.ngoxdb.core.func.impl.StandardSQLFunction;
import org.springframework.stereotype.Service;

import java.sql.SQLException;

/**
 * Oracle数据库方言
 * References:
 * https://docs.oracle.com/cd/B19306_01/gateways.102/b14270/apa.htm
 * <p>
 * Table 6-2 ANSI Data Types Converted to Oracle Data Types
 * https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlqr/Data-Types.html#GUID-219C338B-FE60-422A-B196-2F0A01CAD9A4
 * <p>
 * https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT1832
 * https://www.mat.unical.it/~rullo/teaching/basidati_ev/aa09-10/materialedidatticolab/Oracle%20Datatypes.pdf
 *
 * @author baikai.liao
 * @Time 2021-03-12 10:15:18
 */
@Service
public class OracleDatabaseDialect extends BasicDatabaseDialect implements DatabaseDialect {

    public OracleDatabaseDialect() {

        this.registerColumnType(JdbcDataType.BIT, "NUMBER");
        this.registerColumnType(JdbcDataType.BOOLEAN, "NUMBER");

        this.registerColumnType(JdbcDataType.BINARY, "RAW");
        this.registerColumnType(JdbcDataType.VARBINARY, "RAW");
        this.registerColumnType(JdbcDataType.LONGVARBINARY, "LONG RAW");
        this.registerColumnType(JdbcDataType.BLOB, "LONG RAW");

        this.registerColumnType(JdbcDataType.TINYINT, "NUMBER");
        this.registerColumnType(JdbcDataType.SMALLINT, "NUMBER");
        this.registerColumnType(JdbcDataType.INTEGER, "NUMBER");
        this.registerColumnType(JdbcDataType.BIGINT, "NUMBER");

        this.registerColumnType(JdbcDataType.FLOAT, "NUMBER");
        this.registerColumnType(JdbcDataType.REAL, "NUMBER");
        this.registerColumnType(JdbcDataType.DOUBLE, "NUMBER");

        this.registerColumnType(JdbcDataType.DECIMAL, "NUMBER");
        this.registerColumnType(JdbcDataType.NUMERIC, "NUMBER");

        this.registerColumnType(JdbcDataType.MONEY, "NUMBER");
        this.registerColumnType(JdbcDataType.SMALLMONEY, "NUMBER");


        this.registerColumnType(JdbcDataType.VARCHAR, "VARCHAR2");
        this.registerColumnType(JdbcDataType.NVARCHAR, "NVARCHAR2");

        this.registerColumnType(JdbcDataType.LONGVARCHAR, "LONG");
        this.registerColumnType(JdbcDataType.LONGNVARCHAR, "NCLOB");
        this.registerColumnType(JdbcDataType.CLOB, "CLOB");
        this.registerColumnType(JdbcDataType.NCLOB, "NCLOB");

        this.registerColumnType(JdbcDataType.GUID, "CHAR");

        this.registerColumnType(JdbcDataType.TIME, "DATE");
        this.registerColumnType(JdbcDataType.DATE, "DATE");

        this.registerColumnType(JdbcDataType.TIME_WITH_TIMEZONE, "DATE");
        this.registerColumnType(JdbcDataType.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP WITH TIME ZONE");

        this.registerColumnType(JdbcDataType.TIMESTAMPTZ, "TIMESTAMP WITH TIME ZONE");
        this.registerColumnType(JdbcDataType.TIMESTAMPLTZ, "TIMESTAMP WITH LOCAL TIME ZONE");

        this.registerColumnType(JdbcDataType.DATETIME, "DATE");
        this.registerColumnType(JdbcDataType.DATETIMEOFFSET, "DATE");
        this.registerColumnType(JdbcDataType.SMALLDATETIME, "DATE");

        this.registerColumnType(JdbcDataType.ENUM, "VARCHAR2");
        this.registerColumnType(JdbcDataType.SET, "VARCHAR2");
        this.registerColumnType(JdbcDataType.JSON, "LONG");
        this.registerColumnType(JdbcDataType.YEAR, "DATE");


        // https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/Functions.html#GUID-D079EFD3-C683-441F-977E-2C9503089982
        // 可以对不支持的其他函数对其他数据库提供函数库
        // https://www.cnblogs.com/space-place/p/5378930.html

        // 不支持
        // COSH
        // NANVL
        // REMAINDER
        // SINH
        // TANH

        // 数学函数
        this.registerFunction(MathFunctionEnum.abs, new StandardSQLFunction("abs"));
        this.registerFunction(MathFunctionEnum.ceil, new StandardSQLFunction("ceil"));
        this.registerFunction(MathFunctionEnum.exp, new StandardSQLFunction("exp"));
        this.registerFunction(MathFunctionEnum.floor, new StandardSQLFunction("floor"));
        this.registerFunction(MathFunctionEnum.ln, new StandardSQLFunction("ln"));
        this.registerFunction(MathFunctionEnum.log, new StandardSQLFunction("log"));
        this.registerFunction(MathFunctionEnum.mod, new StandardSQLFunction("mod"));
        this.registerFunction(MathFunctionEnum.pi, new DynamicSQLFunction("acos", JdbcDataType.DOUBLE, "-1"));
        this.registerFunction(MathFunctionEnum.power, new StandardSQLFunction("power"));
        this.registerFunction(MathFunctionEnum.round, new StandardSQLFunction("round"));
        this.registerFunction(MathFunctionEnum.sign, new StandardSQLFunction("sign", JdbcDataType.INTEGER));
        this.registerFunction(MathFunctionEnum.sqrt, new StandardSQLFunction("sqrt"));
        this.registerFunction(MathFunctionEnum.trunc, new StandardSQLFunction("trunc"));
        this.registerFunction(MathFunctionEnum.width_bucket, new StandardSQLFunction("width_bucket"));
        this.registerFunction(MathFunctionEnum.random, new NoArgumentSQLFunction("dbms_random.value", JdbcDataType.DOUBLE, false));
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
        // ADD_MONTHS
        // DBTIMEZONE
        // FROM_TZ
        // MONTHS_BETWEEN
        // NEW_TIME
        // NEXT_DAY
        // NUMTODSINTERVAL
        // NUMTOYMINTERVAL
        // ORA_DST_AFFECTED
        // ORA_DST_CONVERT
        // ORA_DST_ERROR
        // ROUND
        // SESSIONTIMEZONE
        // SYS_EXTRACT_UTC
        // TO_DSINTERVAL
        // TO_TIMESTAMP_TZ
        // TO_YMINTERVAL
        // TRUNC (date)
        // TZ_OFFSET

        // https://aws.amazon.com/cn/blogs/database/converting-the-sysdate-function-from-oracle-to-postgresql/

        this.registerFunction(DateTypeFunctionEnum.to_char, new StandardSQLFunction("to_char"));
        this.registerFunction(DateTypeFunctionEnum.to_date, new StandardSQLFunction("to_date"));
        this.registerFunction(DateTypeFunctionEnum.to_number, new StandardSQLFunction("to_number"));
        this.registerFunction(DateTypeFunctionEnum.to_timestamp, new StandardSQLFunction("to_timestamp"));

        final SQLFunction currentDate = new NoArgumentSQLFunction("current_date", JdbcDataType.TIMESTAMP, false);
        final SQLFunction currentTimestamp = new NoArgumentSQLFunction("current_timestamp", JdbcDataType.TIMESTAMP, false);

        this.registerFunction(DateTypeFunctionEnum.clock_timestamp, currentTimestamp);    // Current date and time (changes during statement execution);

        // CURRENT_DATE and CURRENT_TIMESTAMP return the current date and time in the session time zone.
        this.registerFunction(DateTypeFunctionEnum.current_date, currentDate);
        this.registerFunction(DateTypeFunctionEnum.current_time, currentDate);
        this.registerFunction(DateTypeFunctionEnum.current_timestamp, currentTimestamp);

        this.registerFunction(DateTypeFunctionEnum.extract, new StandardSQLFunction("extract"));
        this.registerFunction(DateTypeFunctionEnum.localtime, new NoArgumentSQLFunction("sysdate", JdbcDataType.TIMESTAMP, false));
        this.registerFunction(DateTypeFunctionEnum.localtimestamp, new NoArgumentSQLFunction("systimestamp", JdbcDataType.TIMESTAMP, false));
        this.registerFunction(DateTypeFunctionEnum.now, currentTimestamp);    // Current date and time (start of current transaction);

        // https://stackoverflow.com/questions/17922106/different-current-timestamp-and-sysdate-in-oracle
        this.registerFunction(DateTypeFunctionEnum.statement_timestamp, currentTimestamp);    // Current date and time (start of current statement);
        this.registerFunction(DateTypeFunctionEnum.transaction_timestamp, currentTimestamp);  // Current date and time (start of current transaction);

        // 最后一天
        this.registerFunction(DateTypeFunctionEnum.last_day, new StandardSQLFunction("last_day"));

        // 带时区
        this.registerFunction(DateTypeFunctionEnum.datetime_offset, new NoArgumentSQLFunction("current_timestamp", JdbcDataType.TIMESTAMP, false));

        // utc类型
        this.registerFunction(DateTypeFunctionEnum.utc_datetime, new NoArgumentSQLFunction("sys_extract_utc(current_timestamp)", JdbcDataType.TIMESTAMP, false));
        this.registerFunction(DateTypeFunctionEnum.utc_date, new NoArgumentSQLFunction("sys_extract_utc(current_timestamp)", JdbcDataType.DATE, false));
        this.registerFunction(DateTypeFunctionEnum.utc_time, new NoArgumentSQLFunction("sys_extract_utc(current_timestamp)", JdbcDataType.TIME, false));

        // 字符函数
        // 不支持
        // NLSSORT
        // REGEXP_REPLACE
        // REGEXP_SUBSTR
        // SOUNDEX
        // TRANSLATE ... USING
        // REGEXP_COUNT
        // REGEXP_INSTR

        // 转换类函数
        // ASCIISTR
        // BIN_TO_NUM
        // CAST
        // CHARTOROWID
        // COMPOSE
        // CONVERT
        // DECOMPOSE
        // HEXTORAW
        // NUMTODSINTERVAL
        // NUMTOYMINTERVAL
        // RAWTOHEX
        // RAWTONHEX
        // ROWIDTOCHAR
        // ROWIDTONCHAR
        // SCN_TO_TIMESTAMP
        // TIMESTAMP_TO_SCN
        // TO_BINARY_DOUBLE
        // TO_BINARY_FLOAT
        // TO_BLOB (bfile)
        // TO_BLOB (raw)
        // TO_CHAR (bfile|blob)
        // TO_CHAR (character)
        // TO_CHAR (datetime)
        // TO_CHAR (number)
        // TO_CLOB (bfile|blob)
        // TO_CLOB (character)
        // TO_DATE
        // TO_DSINTERVAL
        // TO_LOB
        // TO_MULTI_BYTE
        // TO_NCHAR (character)
        // TO_NCHAR (datetime)
        // TO_NCHAR (number)
        // TO_NCLOB
        // TO_NUMBER
        // TO_SINGLE_BYTE
        // TO_TIMESTAMP
        // TO_TIMESTAMP_TZ
        // TO_YMINTERVAL
        // TREAT
        // UNISTR
        // VALIDATE_CONVERSION

        // https://stackoverflow.com/questions/53166804/oracle-11g-12c-lower-turkish-character-problem
        final SQLFunction lengthFunction = new StandardSQLFunction("length");
        this.registerFunction(StringFunctionEnum.bit_length, new DynamicSQLFunction("vsize(?1)*8"));
        this.registerFunction(StringFunctionEnum.char_length, lengthFunction);
        this.registerFunction(StringFunctionEnum.character_length, lengthFunction);
        this.registerFunction(StringFunctionEnum.lower, new StandardSQLFunction("lower"), new StandardSQLFunction("nls_lower"));
        this.registerFunction(StringFunctionEnum.octet_length, lengthFunction);
        this.registerFunction(StringFunctionEnum.position, new DynamicSQLFunction("instr(?1,?2)"));
        this.registerFunction(StringFunctionEnum.trim, new StandardSQLFunction("trim"));
        this.registerFunction(StringFunctionEnum.upper, new StandardSQLFunction("upper"), new StandardSQLFunction("nls_upper"));
        this.registerFunction(StringFunctionEnum.ascii, new StandardSQLFunction("ascii"));
        this.registerFunction(StringFunctionEnum.chr, new StandardSQLFunction("chr"), new StandardSQLFunction("nchr"));
        this.registerFunction(StringFunctionEnum.concat, new StandardSQLFunction("concat"));
        this.registerFunction(StringFunctionEnum.initcap, new StandardSQLFunction("initcap"), new StandardSQLFunction("nls_initcap"));
        this.registerFunction(StringFunctionEnum.length, lengthFunction);
        this.registerFunction(StringFunctionEnum.lpad, new StandardSQLFunction("lpad"));
        this.registerFunction(StringFunctionEnum.ltrim, new StandardSQLFunction("ltrim"));
        this.registerFunction(StringFunctionEnum.replace, new StandardSQLFunction("replace"));
        this.registerFunction(StringFunctionEnum.rpad, new StandardSQLFunction("rpad"));
        this.registerFunction(StringFunctionEnum.rtrim, new StandardSQLFunction("rtrim"));
        this.registerFunction(StringFunctionEnum.substr, new StandardSQLFunction("substr"));
        this.registerFunction(StringFunctionEnum.to_ascii, new StandardSQLFunction("ascii"));
        this.registerFunction(StringFunctionEnum.translate, new StandardSQLFunction("translate"));
        // https://stackoverflow.com/questions/13951576/how-to-generate-a-version-4-random-uuid-on-oracle
        this.registerFunction(StringFunctionEnum.uuid, new NoArgumentSQLFunction("sys_guid", JdbcDataType.VARCHAR));
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
    public PreparedPagination getPreparedPagination(final String tableName,
                                                    final String[] queryColumnNames,
                                                    final String[] orderColumnNames,
                                                    final boolean isPrimaryKeyOrder,
                                                    final int offset,
                                                    final int limit) {
        PreparedPagination preparedPagination = new PreparedPagination();
        Object[] paramValues = new Object[2];
        paramValues[0] = offset + limit;
        paramValues[1] = offset;
        preparedPagination.setParamValues(paramValues);

        preparedPagination.setPreparedSql(
                String.format("SELECT %s FROM ( SELECT ROWNUM RN_, T.* FROM (SELECT * FROM %s ORDER BY %s) T WHERE ROWNUM <= ?) T0 WHERE T0.RN_ > ?",
                        this.join(queryColumnNames, "T0"),
                        this.toLookupName(tableName),
                        this.join(orderColumnNames)));

        return preparedPagination;
    }

    @Override
    public boolean supportsPartitionBy() {
        return true;
    }

    @Override
    public String getNullColumnString() {
        return "";
    }

    @Override
    public boolean supportsSequences() {
        return true;
    }

    @Override
    public String getCreateSequenceString(String sequenceName, long minvalue, long maxvalue, long startWith, long step) {
        StringBuilder sBuilder = new StringBuilder("create sequence ");
        sBuilder.append(this.toLookupName(sequenceName));
        sBuilder.append(" minvalue ").append(minvalue);
        if (maxvalue > 0) {
            sBuilder.append(" maxvalue").append(maxvalue);
        } else {
            sBuilder.append(" nomaxvalue");
        }
        sBuilder.append(" start with ").append(startWith);
        sBuilder.append(" increment by ").append(step);
        return sBuilder.toString();
    }

    @Override
    public String getDropSequenceString(String sequenceName) {
        return "drop sequence " + this.toLookupName(sequenceName);
    }

    @Override
    public boolean supportsIdentityReplaceColumnType() {
        return false;
    }

    @Override
    public boolean supportsCascadeDelete() {
        return true;
    }

    @Override
    public String toBooleanValueString(final Object value, final int jdbcDataType) {
        if (value == null) {
            return null;
        }
        if (jdbcDataType == JdbcDataType.BOOLEAN) {
            if ("true".equals(value)) {
                return "1";
            } else if ("false".equals(value)) {
                return "0";
            }
        } else if (jdbcDataType == JdbcDataType.BIT) {
            if (value instanceof Boolean) {
                return ((Boolean) value) ? "1" : "0";
            } else if (value instanceof Byte) {
                return new Byte("1").equals(value) ? "1" : "0";
            }
        }
        return value.toString();
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
                return 38;
            case JdbcDataType.TIME:
            case JdbcDataType.TIMESTAMP:
            case JdbcDataType.TIMESTAMPTZ:
            case JdbcDataType.TIMESTAMPLTZ:
                return 6;
            case JdbcDataType.CHAR:
            case JdbcDataType.NCHAR:
            case JdbcDataType.RAW:
                return 2000;
            case JdbcDataType.VARCHAR:
            case JdbcDataType.NVARCHAR:
                return 4000;
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
            case JdbcDataType.CHAR:
            case JdbcDataType.NCHAR:
            case JdbcDataType.VARCHAR:
            case JdbcDataType.NVARCHAR:
            case JdbcDataType.BIT:
            case JdbcDataType.RAW:
            case JdbcDataType.FLOAT:
                return 1;
            default:
                return 0;
        }
    }

    @Override
    public int getMaximumScale(int jdbcDataType) {
        // https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlqr/Data-Types.html#GUID-219C338B-FE60-422A-B196-2F0A01CAD9A4
        switch (jdbcDataType) {
            case JdbcDataType.NUMERIC:
            case JdbcDataType.DECIMAL:
                return 0x7F;    // 127
            default:
                return 0;
        }
    }

    @Override
    public int getMinimumScale(int jdbcDataType) {
        // https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlqr/Data-Types.html#GUID-219C338B-FE60-422A-B196-2F0A01CAD9A4
        switch (jdbcDataType) {
            case JdbcDataType.NUMERIC:
            case JdbcDataType.DECIMAL:
                return -84;
            default:
                return 0;
        }
    }

    @Override
    public int getRightDataTypeForVarchar(int columnSize, boolean[] doesIgnoredLength) {
        int maximumPrecision = this.getMaximumPrecision(JdbcDataType.VARCHAR);
        if (columnSize > maximumPrecision) {
            doesIgnoredLength[0] = true;
            return JdbcDataType.CLOB;
        }
        return JdbcDataType.VARCHAR;
    }

    @Override
    public int getRightDataTypeForNVarchar(int columnSize, boolean[] doesIgnoredLength) {
        int maximumPrecision = this.getMaximumPrecision(JdbcDataType.NVARCHAR);
        if (columnSize > maximumPrecision) {
            doesIgnoredLength[0] = true;
            return JdbcDataType.NCLOB;
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
        int maximumPrecision = this.getMaximumPrecision(JdbcDataType.RAW);
        if (columnSize > maximumPrecision) {
            return this.getRightDataTypeForVarBinary(columnSize, doesIgnoredLength);
        }
        return JdbcDataType.RAW;
    }

    @Override
    public int getRightDataTypeForVarBinary(int columnSize, boolean[] doesIgnoredLength) {
        // long raw
        doesIgnoredLength[0] = true;
        return JdbcDataType.LONGVARBINARY;
    }

    @Override
    public boolean supportsDecimalToInt() {
        return false;
    }

    @Override
    public boolean supportsYearToDate() {
        return true;
    }

    @Override
    public boolean supportsJoin() {
        return false;
    }

    @Override
    public Object getSqlGenericType(Object src) throws SQLException {

        if (src instanceof oracle.sql.TIMESTAMP) {
            return ((oracle.sql.TIMESTAMP) src).timestampValue();
        } else if (src instanceof oracle.sql.TIMESTAMPLTZ) {
            return ((oracle.sql.TIMESTAMPLTZ) src).timestampValue();
        } else if (src instanceof oracle.sql.TIMESTAMPTZ) {
            return ((oracle.sql.TIMESTAMPTZ) src).timestampValue();
        }

        return src;
    }

    @Override
    public boolean supportsBitmapIndex() {
        return true;
    }

    @Override
    public String getFullTextIndexString() {
        return "";
    }

    @Override
    public boolean supportsGlobalPartitionIndex() {
        return true;
    }

    @Override
    public boolean supportsLocalPartitionIndex() {
        return true;
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
    public void registerColumnTypeForDouble(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForDouble(ct, ci);
        ct.setPrecision(126);
    }

    @Override
    public void registerColumnTypeForReal(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForDouble(ct, ci);
        ct.setPrecision(23);
    }

    @Override
    public void registerColumnTypeForFloat(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForDouble(ct, ci);
        ct.setPrecision(49);
    }

    @Override
    public void registerColumnTypeForBigInt(ColumnType ct, ColumnInfo ci) {
        ct.setJdbcDataType(JdbcDataType.NUMBER);
        ct.setPrecision(19);
    }

    @Override
    public void registerColumnTypeForInt(ColumnType ct, ColumnInfo ci) {
        ct.setJdbcDataType(JdbcDataType.NUMBER);
        ct.setPrecision(10);
    }

    @Override
    public void registerColumnTypeForSmallint(ColumnType ct, ColumnInfo ci) {
        // 不支持smallint, 用number(5) 代替
        ct.setJdbcDataType(JdbcDataType.NUMBER);
        ct.setPrecision(5);
    }

    @Override
    public void registerColumnTypeForTinyint(ColumnType ct, ColumnInfo ci) {
        // 不支持tinyint, 用number(3) 代替
        ct.setJdbcDataType(JdbcDataType.NUMBER);
        ct.setPrecision(3);
    }

    @Override
    public void registerColumnTypeForBit(ColumnType ct, ColumnInfo ci) {
        // https://stackoverflow.com/questions/2426145/oracles-lack-of-a-bit-datatype-for-table-columns
        // 不支持bit, 用number(1) 代替
        ct.setJdbcDataType(JdbcDataType.NUMBER);
        ct.setPrecision(1);
    }

    @Override
    public boolean supportsRecycleBin() {
        return true;
    }

    @Override
    public String getDropTableString(String tableName) {
        return String.format("drop table %s purge", this.toLookupName(tableName));
    }

    @Override
    public boolean defaultUpperCaseName() {
        return true;
    }
}
