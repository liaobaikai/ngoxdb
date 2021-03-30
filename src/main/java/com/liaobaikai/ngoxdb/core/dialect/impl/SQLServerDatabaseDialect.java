package com.liaobaikai.ngoxdb.core.dialect.impl;

import com.liaobaikai.ngoxdb.bean.ColumnType;
import com.liaobaikai.ngoxdb.bean.PreparedPagination;
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
 * @author baikai.liao
 * @Time 2021-03-14 16:09:02
 */
@Service
public class SQLServerDatabaseDialect extends BasicDatabaseDialect {

    public SQLServerDatabaseDialect() {
        // 注册类型
        // timestamp -> binary
        // https://stackoverflow.com/questions/31377666/why-sql-server-timestamp-type-is-mapped-to-binary-type-in-hibernate

        this.registerColumnType(JdbcDataType.BIT, "bit");
        // https://stackoverflow.com/questions/1777257/how-do-you-create-a-yes-no-boolean-field-in-sql-server
        this.registerColumnType(JdbcDataType.BOOLEAN, "bit");

        this.registerColumnType(JdbcDataType.BINARY, "binary");
        this.registerColumnType(JdbcDataType.VARBINARY, "varbinary");
        this.registerColumnType(JdbcDataType.LONGVARBINARY, "varbinary(max)");
        this.registerColumnType(JdbcDataType.BLOB, "varbinary(max)");

        this.registerColumnType(JdbcDataType.TINYINT, "tinyint");
        this.registerColumnType(JdbcDataType.SMALLINT, "smallint");
        this.registerColumnType(JdbcDataType.INTEGER, "int");
        this.registerColumnType(JdbcDataType.BIGINT, "bigint");

        this.registerColumnType(JdbcDataType.FLOAT, "float");
        this.registerColumnType(JdbcDataType.REAL, "real");
        this.registerColumnType(JdbcDataType.DOUBLE, "float");

        this.registerColumnType(JdbcDataType.DECIMAL, "decimal");
        this.registerColumnType(JdbcDataType.NUMERIC, "decimal");

        this.registerColumnType(JdbcDataType.MONEY, "money");
        this.registerColumnType(JdbcDataType.SMALLMONEY, "smallmoney");

        this.registerColumnType(JdbcDataType.VARCHAR, "varchar");
        this.registerColumnType(JdbcDataType.NVARCHAR, "nvarchar");

        this.registerColumnType(JdbcDataType.LONGVARCHAR, "varchar(max)");
        this.registerColumnType(JdbcDataType.LONGNVARCHAR, "nvarchar(max)");
        this.registerColumnType(JdbcDataType.CLOB, "varchar(max)");
        this.registerColumnType(JdbcDataType.NCLOB, "nvarchar(max)");

        this.registerColumnType(JdbcDataType.GUID, "guid");

        this.registerColumnType(JdbcDataType.TIME, "time");
        this.registerColumnType(JdbcDataType.DATE, "date");

        this.registerColumnType(JdbcDataType.TIME_WITH_TIMEZONE, "time");
        this.registerColumnType(JdbcDataType.TIMESTAMP_WITH_TIMEZONE, "datetime2");

        this.registerColumnType(JdbcDataType.TIMESTAMP, "datetime2");
        this.registerColumnType(JdbcDataType.TIMESTAMPTZ, "datetime2");
        this.registerColumnType(JdbcDataType.TIMESTAMPLTZ, "datetime2");

        this.registerColumnType(JdbcDataType.DATETIME, "datetime");
        this.registerColumnType(JdbcDataType.DATETIMEOFFSET, "datetimeoffset");
        this.registerColumnType(JdbcDataType.SMALLDATETIME, "smalldatetime");

        this.registerColumnType(JdbcDataType.ENUM, "varchar");
        this.registerColumnType(JdbcDataType.SET, "varchar");
        this.registerColumnType(JdbcDataType.JSON, "varchar(max)");
        this.registerColumnType(JdbcDataType.YEAR, "date");

        // 数学函数
        // https://docs.microsoft.com/en-us/sql/t-sql/functions/mathematical-functions-transact-sql?view=sql-server-ver15
        // 不支持：
        // SQUARE (平方) --> 用 POWER(n, 2) 代替
        this.registerFunction(MathFunctionEnum.abs, new StandardSQLFunction("abs"));
        this.registerFunction(MathFunctionEnum.ceiling, new StandardSQLFunction("ceiling"));
        this.registerFunction(MathFunctionEnum.degrees, new StandardSQLFunction("degrees"));
        this.registerFunction(MathFunctionEnum.exp, new StandardSQLFunction("exp"));
        this.registerFunction(MathFunctionEnum.floor, new StandardSQLFunction("floor"));
        this.registerFunction(MathFunctionEnum.log, new StandardSQLFunction("log"));
        this.registerFunction(MathFunctionEnum.log10, new StandardSQLFunction("log10"));
        this.registerFunction(MathFunctionEnum.pi, new StandardSQLFunction("pi"));
        this.registerFunction(MathFunctionEnum.power, new StandardSQLFunction("power"));
        this.registerFunction(MathFunctionEnum.radians, new StandardSQLFunction("radians"));
        this.registerFunction(MathFunctionEnum.round, new StandardSQLFunction("round"));
        this.registerFunction(MathFunctionEnum.sign, new StandardSQLFunction("sign"));
        this.registerFunction(MathFunctionEnum.sqrt, new StandardSQLFunction("sqrt"));
        this.registerFunction(MathFunctionEnum.random, new StandardSQLFunction("rand"));
        this.registerFunction(MathFunctionEnum.acos, new StandardSQLFunction("acos"));
        this.registerFunction(MathFunctionEnum.asin, new StandardSQLFunction("asin"));
        this.registerFunction(MathFunctionEnum.atan, new StandardSQLFunction("atan"));
        this.registerFunction(MathFunctionEnum.atan2, new StandardSQLFunction("atn2"));
        this.registerFunction(MathFunctionEnum.cos, new StandardSQLFunction("cos"));
        this.registerFunction(MathFunctionEnum.cot, new StandardSQLFunction("cot"));
        this.registerFunction(MathFunctionEnum.sin, new StandardSQLFunction("sin"));
        this.registerFunction(MathFunctionEnum.tan, new StandardSQLFunction("tan"));

        // 日期函数
        // https://docs.microsoft.com/en-us/sql/t-sql/functions/date-and-time-data-types-and-functions-transact-sql?view=sql-server-ver15
        // 不支持
        // DATENAME
        // DAY
        // MONTH
        // YEAR
        // DATEDIFF_BIG
        // DATEADD
        // EOMONTH
        // SWITCHOFFSET
        // TODATETIMEOFFSET
        //

        SQLFunction currentTimestamp = new NoArgumentSQLFunction("current_timestamp", JdbcDataType.TIMESTAMP, false);
        SQLFunction localDatetime = new NoArgumentSQLFunction("sysdatetime", JdbcDataType.TIMESTAMP);

        this.registerFunction(DateTypeFunctionEnum.age, new StandardSQLFunction("datediff"));
        this.registerFunction(DateTypeFunctionEnum.clock_timestamp, currentTimestamp);
        this.registerFunction(DateTypeFunctionEnum.current_date, currentTimestamp);
        this.registerFunction(DateTypeFunctionEnum.current_time, currentTimestamp);
        this.registerFunction(DateTypeFunctionEnum.current_timestamp, currentTimestamp);
        this.registerFunction(DateTypeFunctionEnum.date_part, new StandardSQLFunction("datepart"));
        this.registerFunction(DateTypeFunctionEnum.localtime, localDatetime);
        this.registerFunction(DateTypeFunctionEnum.localtimestamp, localDatetime, new NoArgumentSQLFunction("getdate", JdbcDataType.TIMESTAMP));
        this.registerFunction(DateTypeFunctionEnum.make_date,
                new StandardSQLFunction("timefromparts"),
                new StandardSQLFunction("datefromparts"),
                new StandardSQLFunction("datetimefromparts"),
                new StandardSQLFunction("smalldatetimefromparts"),
                new StandardSQLFunction("datetimeoffsetfromparts"),
                new StandardSQLFunction("datetime2fromparts"));
        this.registerFunction(DateTypeFunctionEnum.now, currentTimestamp);
        this.registerFunction(DateTypeFunctionEnum.statement_timestamp, currentTimestamp);
        this.registerFunction(DateTypeFunctionEnum.timeofday, currentTimestamp);
        this.registerFunction(DateTypeFunctionEnum.transaction_timestamp, currentTimestamp);

        // 带时区
        this.registerFunction(DateTypeFunctionEnum.datetime_offset, new NoArgumentSQLFunction("sysdatetimeoffset", JdbcDataType.TIMESTAMP));
        // utc 时间
        this.registerFunction(DateTypeFunctionEnum.utc_datetime, new NoArgumentSQLFunction("sysutcdatetime", JdbcDataType.TIMESTAMP));
        this.registerFunction(DateTypeFunctionEnum.utc_date, new NoArgumentSQLFunction("getutcdate", JdbcDataType.DATE));
        this.registerFunction(DateTypeFunctionEnum.utc_time, new NoArgumentSQLFunction("sysutcdatetime", JdbcDataType.TIME));

        // 字符串函数
        // https://docs.microsoft.com/en-us/sql/t-sql/functions/string-functions-transact-sql?view=sql-server-ver15
        // 不支持
        // CHARINDEX
        // DIFFERENCE
        // PATINDEX
        // QUOTENAME  ---> 转换成SQLServer特有的字符串标识 [xxxx]
        // SOUNDEX
        // SPACE
        // STR
        // STRING_AGG       // SQL Server 2017 (14.x) and later
        // STRING_ESCAPE    // SQL Server 2016 (13.x) and later
        // STRING_SPLIT     // SQL Server 2016 and later
        // STUFF ---> 替换
        // UNICODE

        this.registerFunction(StringFunctionEnum.lower, new StandardSQLFunction("lower"));
        this.registerFunction(StringFunctionEnum.substring, new StandardSQLFunction("substring"));
        this.registerFunction(StringFunctionEnum.trim, new StandardSQLFunction("trim"));
        this.registerFunction(StringFunctionEnum.upper, new StandardSQLFunction("upper"));
        this.registerFunction(StringFunctionEnum.ascii, new StandardSQLFunction("ascii"));
        this.registerFunction(StringFunctionEnum.chr,
                new StandardSQLFunction("char"),
                new StandardSQLFunction("nchar"));
        this.registerFunction(StringFunctionEnum.concat, new StandardSQLFunction("concat"));
        this.registerFunction(StringFunctionEnum.concat_ws, new StandardSQLFunction("concat_ws"));
        this.registerFunction(StringFunctionEnum.left, new StandardSQLFunction("left"));
        this.registerFunction(StringFunctionEnum.length, new StandardSQLFunction("len"));
        this.registerFunction(StringFunctionEnum.ltrim, new StandardSQLFunction("ltrim"));
        this.registerFunction(StringFunctionEnum.repeat, new StandardSQLFunction("replicate"));
        this.registerFunction(StringFunctionEnum.replace, new StandardSQLFunction("replace"));
        this.registerFunction(StringFunctionEnum.reverse, new StandardSQLFunction("reverse"));
        this.registerFunction(StringFunctionEnum.right, new StandardSQLFunction("right"));
        this.registerFunction(StringFunctionEnum.rtrim, new StandardSQLFunction("rtrim"));
        this.registerFunction(StringFunctionEnum.translate, new StandardSQLFunction("translate"));   // SQL Server 2017 (14.x) and later
        this.registerFunction(StringFunctionEnum.uuid, new NoArgumentSQLFunction("newid", JdbcDataType.VARCHAR));


    }

    @Override
    public char openQuote() {
        return '[';
    }

    @Override
    public char closeQuote() {
        return ']';
    }

    @Override
    public boolean supportsCommentOn() {
        return true;
    }

    @Override
    public String getTableCommentString(String tableName, String tableComment) {
        return String.format("exec sys.sp_addextendedproperty @name=N'MS_Description', @value=N'%s' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'%s'", tableComment, tableName);
    }

    @Override
    public String getColumnCommentString(String tableName, String columnName, String columnComment) {
        return String.format("exec sys.sp_addextendedproperty @name=N'MS_Description', @value=N'%s' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'%s', @level2type=N'COLUMN',@level2name=N'%s'", columnComment, tableName, columnName);
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
        if (offset == 0) {
            preparedPagination.setPreparedSql(String.format("select top %s %s from %s t order by %s",
                    limit,
                    this.join(queryColumnNames, "t"),
                    this.toLookupName(tableName),
                    this.join(orderColumnNames)));
        } else {
            Object[] paramValues = new Object[1];
            paramValues[0] = offset;
            preparedPagination.setParamValues(paramValues);

            preparedPagination.setPreparedSql(String.format("select top %s %s from ( select *, row_number() over( order by %s ) as _row_number from %s ) t where t._row_number > ?",
                    limit,
                    this.join(queryColumnNames, "t"),
                    this.join(orderColumnNames),
                    this.toLookupName(tableName)
            ));
        }

        return preparedPagination;
    }

    @Override
    public boolean supportsPartitionBy() {
        return true;
    }

    @Override
    public boolean supportsIdentityColumns() {
        return true;
    }

    @Override
    public String getIdentityColumnString() {
        return "identity";
    }

    @Override
    public boolean supportsDisableIdentity() {
        return true;
    }

    @Override
    public String getDisableIdentityString(String tableName) {
        // https://docs.microsoft.com/zh-cn/sql/t-sql/statements/set-identity-insert-transact-sql?view=sql-server-ver15
        // https://blog.csdn.net/suo082407128/article/details/56283446
        // 似乎这个命令是针对于数据库的，不是针对表的、
        // 当有表设置过"set identity_insert %s on"的时候，会抛出错误，需要将之前设置的关闭。
        return String.format("set identity_insert %s on", tableName);
    }

    @Override
    public String getEnableIdentityString(String tableName) {
        // https://docs.microsoft.com/zh-cn/sql/t-sql/statements/set-identity-insert-transact-sql?view=sql-server-ver15
        return String.format("set identity_insert %s off", tableName);

    }

    @Override
    public String toBooleanValueString(Object value, int jdbcDataType) {

        if (value == null) {
            return null;
        }

        if (!(jdbcDataType == JdbcDataType.BIT || jdbcDataType == JdbcDataType.BOOLEAN)) {
            return value.toString();
        }

        if (value instanceof Boolean) {
            return ((Boolean) value) ? "1" : "0";
        } else if (value instanceof String) {
            if ("1".equals(value.toString()) || "true".equalsIgnoreCase(value.toString())) {
                return "1";
            } else {
                return "0";
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
            case JdbcDataType.BIT:
                return 1;
            case JdbcDataType.NUMERIC:
            case JdbcDataType.DECIMAL:
                return 38;
            case JdbcDataType.TIME:
            case JdbcDataType.TIMESTAMP:
            case JdbcDataType.TIMESTAMPTZ:
                return 6;
            case JdbcDataType.NCHAR:
            case JdbcDataType.NVARCHAR:
                return 4000;
            case JdbcDataType.CHAR:
            case JdbcDataType.VARCHAR:
            case JdbcDataType.BINARY:
            case JdbcDataType.VARBINARY:
                return 8000;
            case JdbcDataType.LONGNVARCHAR:
            case JdbcDataType.NCLOB:
                // assert MAX_VARTYPE_MAX_CHARS == NTEXT_MAX_CHARS;
                return 0x3FFFFFFF;
            case JdbcDataType.LONGVARCHAR:
            case JdbcDataType.LONGVARBINARY:
                // assert MAX_VARTYPE_MAX_BYTES == IMAGE_TEXT_MAX_BYTES;
                return 0x7FFFFFFF;
            default:
                return 0;
        }
    }

    @Override
    public int getMinimumPrecision(int jdbcDataType) {
        switch (jdbcDataType) {
            case JdbcDataType.BIT:
            case JdbcDataType.CHAR:
            case JdbcDataType.VARCHAR:
            case JdbcDataType.BINARY:
            case JdbcDataType.VARBINARY:
            case JdbcDataType.NUMERIC:
            case JdbcDataType.DECIMAL:
            case JdbcDataType.NCHAR:
            case JdbcDataType.NVARCHAR:
            case JdbcDataType.LONGNVARCHAR:
            case JdbcDataType.NCLOB:
            case JdbcDataType.LONGVARCHAR:
            case JdbcDataType.LONGVARBINARY:
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
        if (columnSize > maximumPrecision) {      // > 8000
            doesIgnoredLength[0] = true;
            return JdbcDataType.LONGVARCHAR;    // varchar(max)
        }
        return JdbcDataType.VARCHAR;
    }

    @Override
    public int getRightDataTypeForNVarchar(int columnSize, boolean[] doesIgnoredLength) {
        int maximumPrecision = this.getMaximumPrecision(JdbcDataType.NVARCHAR);
        if (columnSize > maximumPrecision) {      // > 4000
            doesIgnoredLength[0] = true;
            return JdbcDataType.LONGNVARCHAR;   // nvarchar(max)
        }
        return JdbcDataType.NVARCHAR;
    }

    @Override
    public int getRightDataTypeForChar(int columnSize, boolean[] doesIgnoredLength) {
        int maximumPrecision = this.getMaximumPrecision(JdbcDataType.CHAR);
        if (columnSize > maximumPrecision) {      // > 8000
            doesIgnoredLength[0] = true;
            return JdbcDataType.LONGVARCHAR;    // varchar(max)
        }
        return JdbcDataType.CHAR;
    }

    @Override
    public int getRightDataTypeForNChar(int columnSize, boolean[] doesIgnoredLength) {
        int maximumPrecision = this.getMaximumPrecision(JdbcDataType.NCHAR);
        if (columnSize > maximumPrecision) {          // > 4000
            doesIgnoredLength[0] = true;
            return JdbcDataType.LONGNVARCHAR;       // nvarchar(max)
        }
        return JdbcDataType.NCHAR;
    }

    @Override
    public int getRightDataTypeForBinary(int columnSize, boolean[] doesIgnoredLength) {
        int maximumPrecision = this.getMaximumPrecision(JdbcDataType.BINARY);
        if (columnSize > maximumPrecision) {          // > 8000
            doesIgnoredLength[0] = true;
            return JdbcDataType.LONGVARBINARY;      // varbinary(max)
        }
        return JdbcDataType.BINARY;
    }

    @Override
    public int getRightDataTypeForVarBinary(int columnSize, boolean[] doesIgnoredLength) {
        int maximumPrecision = this.getMaximumPrecision(JdbcDataType.VARBINARY);
        if (columnSize > maximumPrecision) {          // > 8000
            doesIgnoredLength[0] = true;
            return JdbcDataType.LONGVARBINARY;      // varbinary(max)
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
        return "FULLTEXT";
    }

    @Override
    public boolean supportsSpatialIndex() {
        return false;
    }

    @Override
    public boolean supportsClusteredIndex() {
        return true;
    }

    @Override
    public void registerColumnTypeForYear(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForYear(ct, ci);
    }

    @Override
    public void registerColumnTypeForJson(ColumnType ct, ColumnInfo ci) {
        this.registerColumnTypeForLongNVarchar(ct, ci);
    }

    @Override
    public void registerColumnTypeForSet(ColumnType ct, ColumnInfo ci) {
        this.registerColumnTypeForNVarchar(ct, ci);
    }

    @Override
    public void registerColumnTypeForEnum(ColumnType ct, ColumnInfo ci) {
        this.registerColumnTypeForNVarchar(ct, ci);
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
        ct.setTypeName("uniqueidentifier");
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
        ct.setTypeName("xml");
    }

    @Override
    public void registerColumnTypeForNClob(ColumnType ct, ColumnInfo ci) {
        this.registerColumnTypeForLongNVarchar(ct, ci);
    }

    @Override
    public void registerColumnTypeForLongNVarchar(ColumnType ct, ColumnInfo ci) {
        if ("xml".equals(ci.getTypeName())) {
            ct.setTypeName("xml");
            return;
        }
        ct.setTypeName("nvarchar(max)");
    }

    @Override
    public void registerColumnTypeForNVarchar(ColumnType ct, ColumnInfo ci) {
        boolean[] doesIgnoredLength = new boolean[1];
        int jdbcDataType = this.getRightDataTypeForNVarchar(ci.getColumnSize(), doesIgnoredLength);

        if (jdbcDataType == JdbcDataType.LONGNVARCHAR && !doesIgnoredLength[0]) {
            // nvarchar(max)
            ct.setTypeName("nvarchar(max)");
        } else if (!doesIgnoredLength[0]) {
            ct.setLength(ci.getColumnSize());
        }
    }

    @Override
    public void registerColumnTypeForNChar(ColumnType ct, ColumnInfo ci) {
        boolean[] doesIgnoredLength = new boolean[1];
        int jdbcDataType = this.getRightDataTypeForNChar(ci.getColumnSize(), doesIgnoredLength);

        if (jdbcDataType == JdbcDataType.LONGNVARCHAR && !doesIgnoredLength[0]) {
            // nvarchar(max)
            ct.setTypeName("nvarchar(max)");
        } else if (!doesIgnoredLength[0]) {
            ct.setLength(ci.getColumnSize());
        }
    }

    @Override
    public void registerColumnTypeForRowId(ColumnType ct, ColumnInfo ci) {
        super.registerColumnTypeForRowId(ct, ci);
    }

    @Override
    public void registerColumnTypeForBoolean(ColumnType ct, ColumnInfo ci) {
        // https://stackoverflow.com/questions/1777257/how-do-you-create-a-yes-no-boolean-field-in-sql-server
        this.registerColumnTypeForBit(ct, ci);
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
        this.registerColumnTypeForLongVarchar(ct, ci);
    }

    @Override
    public void registerColumnTypeForBlob(ColumnType ct, ColumnInfo ci) {
        this.registerColumnTypeForLongVarbinary(ct, ci);
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
        if ("image".equals(ci.getTypeName())) {
            ct.setTypeName("image");
            return;
        }

        ct.setTypeName("varbinary(max)");
    }

    @Override
    public void registerColumnTypeForVarbinary(ColumnType ct, ColumnInfo ci) {

        if ("udt".equals(ci.getTypeName())) {
            ct.setTypeName("udt");
            return;
        }
        // 不可为空的 timestamp 列在语义上等同于 binary(8) 列。
        else if ("timestamp".equals(ci.getTypeName())) {
            ct.setTypeName("timestamp");
            return;
        }

        boolean[] doesIgnoredLength = new boolean[1];
        int jdbcDataType = this.getRightDataTypeForVarBinary(ci.getColumnSize(), doesIgnoredLength);

        if (jdbcDataType == JdbcDataType.LONGVARBINARY && !doesIgnoredLength[0]) {
            // varbinary(max)
            ct.setTypeName("varbinary(max)");
        } else if (!doesIgnoredLength[0]) {
            ct.setLength(ci.getColumnSize());
        }
    }

    @Override
    public void registerColumnTypeForBinary(ColumnType ct, ColumnInfo ci) {
        // 不可为空的 timestamp 列在语义上等同于 binary(8) 列。
        if ("timestamp".equals(ci.getTypeName())) {
            ct.setTypeName("timestamp");
            return;
        }

        boolean[] doesIgnoredLength = new boolean[1];
        int jdbcDataType = this.getRightDataTypeForBinary(ci.getColumnSize(), doesIgnoredLength);

        if (jdbcDataType == JdbcDataType.LONGVARBINARY && !doesIgnoredLength[0]) {
            // varbinary(max)
            ct.setTypeName("varbinary(max)");
        } else if (!doesIgnoredLength[0]) {
            ct.setLength(ci.getColumnSize());
        }
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
        // https://stackoverflow.com/questions/834788/using-varcharmax-vs-text-on-sql-server
        ct.setTypeName("varchar(max)");
    }

    @Override
    public void registerColumnTypeForVarchar(ColumnType ct, ColumnInfo ci) {

        if ("uniqueidentifier".equals(ci.getTypeName())) {
            ct.setTypeName("uniqueidentifier");
            return;
        }

        boolean[] doesIgnoredLength = new boolean[1];
        int jdbcDataType = this.getRightDataTypeForVarchar(ci.getColumnSize(), doesIgnoredLength);

        if (jdbcDataType == JdbcDataType.LONGVARCHAR && !doesIgnoredLength[0]) {
            // varchar(max)
            ct.setTypeName(ct.getTypeName() + "(max)");
        } else if (!doesIgnoredLength[0]) {
            ct.setLength(ci.getColumnSize());
        }
    }

    @Override
    public void registerColumnTypeForChar(ColumnType ct, ColumnInfo ci) {

        if ("uniqueidentifier".equals(ci.getTypeName())) {
            ct.setTypeName("uniqueidentifier");
            return;
        }

        boolean[] doesIgnoredLength = new boolean[1];
        int jdbcDataType = this.getRightDataTypeForChar(ci.getColumnSize(), doesIgnoredLength);

        if (jdbcDataType == JdbcDataType.LONGVARCHAR && !doesIgnoredLength[0]) {
            // varchar(max)
            ct.setTypeName(ct.getTypeName() + "(max)");
        } else if (!doesIgnoredLength[0]) {
            ct.setLength(ci.getColumnSize());
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
    }

    @Override
    public boolean supportsParallelExecute() {
        return false;
    }
}
