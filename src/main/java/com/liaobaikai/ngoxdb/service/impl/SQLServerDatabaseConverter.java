package com.liaobaikai.ngoxdb.service.impl;

import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
import com.liaobaikai.ngoxdb.config.DatabaseConfig;
import com.liaobaikai.ngoxdb.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.dao.impl.SQLServerDatabaseDao;
import com.liaobaikai.ngoxdb.enums.DatabaseVendorEnum;
import com.liaobaikai.ngoxdb.enums.fn.DatabaseFunctionEnum;
import com.liaobaikai.ngoxdb.enums.fn.DateTypeFunctionEnum;
import com.liaobaikai.ngoxdb.enums.fn.MathematicalFunctionEnum;
import com.liaobaikai.ngoxdb.enums.fn.StringFunctionEnum;
import com.liaobaikai.ngoxdb.info.ColumnInfo;
import com.liaobaikai.ngoxdb.info.TableInfo;
import com.liaobaikai.ngoxdb.service.BasicDatabaseConverter;
import com.liaobaikai.ngoxdb.types.SQLServerType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Microsoft SQLServer 转换器
 * @author baikai.liao
 * @Time 2021-01-28 15:56:41
 */
@Slf4j
@Service
public class SQLServerDatabaseConverter extends BasicDatabaseConverter {

    private final SQLServerDatabaseDao databaseDao;

    public SQLServerDatabaseConverter() {
        this.databaseDao = null;
    }

    public SQLServerDatabaseConverter(JdbcTemplate2 jdbcTemplate,
                                      boolean isMaster,
                                      String masterDatabaseVendor,
                                      DatabaseConfig databaseConfig) {
        super(jdbcTemplate, isMaster, masterDatabaseVendor, databaseConfig);
        this.databaseDao = new SQLServerDatabaseDao(jdbcTemplate);
    }


    @Override
    public List<ColumnInfo> getColumns(String... tableName) {
        List<ColumnInfo> columns = super.getColumns(tableName);

        columns.forEach(column -> {

            // 生成columnType.
            StringBuilder columnType = new StringBuilder();
            this.handleDataType(columnType, column);
            column.setColumnType(columnType.toString());

            if (this.isSameDatabaseVendor()) {
                return;
            }

            // 替换掉最外层双括号，如：((1)) ((getdate()))
            if(column.getColumnDef() != null){
                String s = column.getColumnDef()
                        .replaceFirst("^\\(", "")
                        .replaceFirst("\\)$", "");
                if(s.charAt(0) == '(') {
                    s = s.replaceFirst("^\\(", "").replaceFirst("\\)$", "");
                }
                column.setColumnDef(s);
            }

            // 其他数据库不支持以下数据库的类型
            switch (column.getDataType()) {
                case microsoft.sql.Types.DATETIMEOFFSET:
                case microsoft.sql.Types.DATETIME:
                case microsoft.sql.Types.SMALLDATETIME:
                    column.setDataType(Types.TIMESTAMP);
                    break;
                case microsoft.sql.Types.STRUCTURED:
                    // STRUCTURED
                    break;

                case microsoft.sql.Types.MONEY:
                case microsoft.sql.Types.SMALLMONEY:
                    column.setDataType(Types.DECIMAL);
                    break;
                case microsoft.sql.Types.GUID:
                    column.setDataType(Types.VARCHAR);
                    break;

                case microsoft.sql.Types.SQL_VARIANT:
                case microsoft.sql.Types.GEOMETRY:
                case microsoft.sql.Types.GEOGRAPHY:
                    column.setDataType(Types.OTHER);
                    break;
            }
        });
        return columns;
    }

    @Override
    public List<String> getUnsupportedFunctions() {
        return new ArrayList<String>(){
            {
                add("SQUARE");

                add("DAY");
                add("MONTH");
                add("YEAR");
                add("DATEFROMPARTS");
                add("DATETIME2FROMPARTS");
                add("DATETIMEFROMPARTS");
                add("DATETIMEOFFSETFROMPARTS");
                add("SMALLDATETIMEFROMPARTS");
                add("TIMEFROMPARTS");
                add("DATEDIFF_BIG");
                add("DATEADD");
                add("EOMONTH");
                add("SWITCHOFFSET");
                add("TODATETIMEOFFSET");

                add("CHARINDEX");
                add("DIFFERENCE");
                add("PATINDEX");
                add("QUOTENAME");
                add("SOUNDEX");
                add("SPACE");
                add("STR");
                add("STRING_AGG");
                add("STRING_ESCAPE");
                add("STRING_SPLIT");
                add("STUFF");
                add("UNICODE");
            }
        };
    }

    @Override
    public void buildDatabaseFunctionMap(Map<DatabaseFunctionEnum, String[]> map) {
        // 数学函数
        // https://docs.microsoft.com/en-us/sql/t-sql/functions/mathematical-functions-transact-sql?view=sql-server-ver15
        // 不支持：
        // SQUARE (平方) --> 用 POWER(n, 2) 代替
        map.put(MathematicalFunctionEnum.abs, new String[]{"ABS"});
        // map.put(MathematicalFunctionEnum.cbrt, new String[]{""});
        // map.put(MathematicalFunctionEnum.ceil, new String[]{""});
        map.put(MathematicalFunctionEnum.ceiling, new String[]{"CEILING"});
        map.put(MathematicalFunctionEnum.degrees, new String[]{"DEGREES"});
        // map.put(MathematicalFunctionEnum.div, new String[]{""});
        map.put(MathematicalFunctionEnum.exp, new String[]{"EXP"});
        map.put(MathematicalFunctionEnum.factorial, new String[]{""});
        map.put(MathematicalFunctionEnum.floor, new String[]{"FLOOR"});
        // map.put(MathematicalFunctionEnum.gcd, new String[]{""});
        // map.put(MathematicalFunctionEnum.lcm, new String[]{""});
        // map.put(MathematicalFunctionEnum.ln, new String[]{""});
        map.put(MathematicalFunctionEnum.log, new String[]{"LOG"});
        map.put(MathematicalFunctionEnum.log10, new String[]{"LOG10"});
        // map.put(MathematicalFunctionEnum.min_scale, new String[]{""});
        // map.put(MathematicalFunctionEnum.mod, new String[]{""});
        map.put(MathematicalFunctionEnum.pi, new String[]{"PI"});
        map.put(MathematicalFunctionEnum.power, new String[]{"POWER"});
        map.put(MathematicalFunctionEnum.radians, new String[]{"RADIANS"});
        map.put(MathematicalFunctionEnum.round, new String[]{"ROUND"});
        // map.put(MathematicalFunctionEnum.scale, new String[]{""});
        map.put(MathematicalFunctionEnum.sign, new String[]{"SIGN"});
        map.put(MathematicalFunctionEnum.sqrt, new String[]{"SQRT"});
        // map.put(MathematicalFunctionEnum.trim_scale, new String[]{""});
        // map.put(MathematicalFunctionEnum.trunc, new String[]{""});
        map.put(MathematicalFunctionEnum.width_bucket, new String[]{""});
        map.put(MathematicalFunctionEnum.random, new String[]{"RAND"});
        map.put(MathematicalFunctionEnum.acos, new String[]{"ACOS"});
        // map.put(MathematicalFunctionEnum.acosd, new String[]{""});
        map.put(MathematicalFunctionEnum.asin, new String[]{"ASIN"});
        // map.put(MathematicalFunctionEnum.asind, new String[]{""});
        map.put(MathematicalFunctionEnum.atan, new String[]{"ATAN"});
        // map.put(MathematicalFunctionEnum.atand, new String[]{""});
        map.put(MathematicalFunctionEnum.atan2, new String[]{"ATN2"});
        // map.put(MathematicalFunctionEnum.atan2d, new String[]{""});
        map.put(MathematicalFunctionEnum.cos, new String[]{"COS"});
        // map.put(MathematicalFunctionEnum.cosd, new String[]{""});
        map.put(MathematicalFunctionEnum.cot, new String[]{"COT"});
        // map.put(MathematicalFunctionEnum.cotd, new String[]{""});
        map.put(MathematicalFunctionEnum.sin, new String[]{"SIN"});
        // map.put(MathematicalFunctionEnum.sind, new String[]{""});
        map.put(MathematicalFunctionEnum.tan, new String[]{"TAN"});
        // map.put(MathematicalFunctionEnum.tand, new String[]{""});

        // 日期函数
        // https://docs.microsoft.com/en-us/sql/t-sql/functions/date-and-time-data-types-and-functions-transact-sql?view=sql-server-ver15
        // 不支持
        // DATENAME
        // DAY
        // MONTH
        // YEAR
        // DATEFROMPARTS
        // DATETIME2FROMPARTS
        // DATETIMEFROMPARTS
        // DATETIMEOFFSETFROMPARTS
        // SMALLDATETIMEFROMPARTS
        // TIMEFROMPARTS
        // DATEDIFF_BIG
        // DATEADD
        // EOMONTH
        // SWITCHOFFSET
        // TODATETIMEOFFSET
        //

        map.put(DateTypeFunctionEnum.to_char, new String[]{"FORMAT"});
        // map.put(DateTypeFunctionEnum.to_date, new String[]{""});
        // map.put(DateTypeFunctionEnum.to_number, new String[]{""});
        // map.put(DateTypeFunctionEnum.to_timestamp, new String[]{""});

        map.put(DateTypeFunctionEnum.age, new String[]{"DATEDIFF"});
        map.put(DateTypeFunctionEnum.clock_timestamp, new String[]{"SYSDATETIME()"});
        map.put(DateTypeFunctionEnum.current_date, new String[]{"GETDATE()", "GETUTCDATE"});
        map.put(DateTypeFunctionEnum.current_time, new String[]{"SYSDATETIME()"});
        map.put(DateTypeFunctionEnum.current_timestamp, new String[]{"CURRENT_TIMESTAMP"});
        map.put(DateTypeFunctionEnum.date_part, new String[]{"DATEPART()"});
        // map.put(DateTypeFunctionEnum.date_trunc, new String[]{""});
        // map.put(DateTypeFunctionEnum.extract, new String[]{""});
        // map.put(DateTypeFunctionEnum.isfinite, new String[]{""});
        // map.put(DateTypeFunctionEnum.justify_days, new String[]{""});
        // map.put(DateTypeFunctionEnum.justify_hours, new String[]{""});
        // map.put(DateTypeFunctionEnum.justify_interval, new String[]{""});
        map.put(DateTypeFunctionEnum.localtime, new String[]{"SYSDATETIME()"});
        map.put(DateTypeFunctionEnum.localtimestamp, new String[]{"SYSDATETIME()"});
        map.put(DateTypeFunctionEnum.make_date, new String[]{"DATEFROMPARTS"});
        // map.put(DateTypeFunctionEnum.make_interval, new String[]{""});
        // map.put(DateTypeFunctionEnum.make_time, new String[]{""});
        // map.put(DateTypeFunctionEnum.make_timestamp, new String[]{""});
        // map.put(DateTypeFunctionEnum.make_timestamptz, new String[]{""});
        map.put(DateTypeFunctionEnum.now, new String[]{"SYSDATETIME()", "SYSDATETIMEOFFSET()", "SYSUTCDATETIME()"});  // SQL Server 2019
        map.put(DateTypeFunctionEnum.statement_timestamp, new String[]{"SYSDATETIME()"});
        map.put(DateTypeFunctionEnum.timeofday, new String[]{"SYSDATETIME()"});
        map.put(DateTypeFunctionEnum.transaction_timestamp, new String[]{"SYSDATETIME()"});

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

        // map.put(StringFunctionEnum.bit_length, new String[]{""});
        // map.put(StringFunctionEnum.char_length, new String[]{""});
        // map.put(StringFunctionEnum.character_length, new String[]{""});
        map.put(StringFunctionEnum.lower, new String[]{"LOWER"});
        // map.put(StringFunctionEnum.normalize, new String[]{""});
        // map.put(StringFunctionEnum.octet_length, new String[]{""});
        // map.put(StringFunctionEnum.overlay, new String[]{""});
        // map.put(StringFunctionEnum.position, new String[]{""});
        map.put(StringFunctionEnum.substring, new String[]{"SUBSTRING"});
        map.put(StringFunctionEnum.trim, new String[]{"TRIM"});
        map.put(StringFunctionEnum.upper, new String[]{"UPPER"});
        map.put(StringFunctionEnum.ascii, new String[]{"ASCII"});
        // map.put(StringFunctionEnum.btrim, new String[]{""});
        map.put(StringFunctionEnum.chr, new String[]{"CHAR", "NCHAR"});
        map.put(StringFunctionEnum.concat, new String[]{"CONCAT"});
        map.put(StringFunctionEnum.concat_ws, new String[]{"CONCAT_WS"});
        // map.put(StringFunctionEnum.format, new String[]{"FORMAT"}); // 存在差异化
        // map.put(StringFunctionEnum.initcap, new String[]{""});
        map.put(StringFunctionEnum.left, new String[]{"LEFT"});
        map.put(StringFunctionEnum.length, new String[]{"LEN"});
        // map.put(StringFunctionEnum.lpad, new String[]{""});
        map.put(StringFunctionEnum.ltrim, new String[]{"LTRIM"});
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
        map.put(StringFunctionEnum.repeat, new String[]{"REPLICATE"});
        map.put(StringFunctionEnum.replace, new String[]{"REPLACE"});
        map.put(StringFunctionEnum.reverse, new String[]{"REVERSE"});
        map.put(StringFunctionEnum.right, new String[]{"RIGHT"});
        // map.put(StringFunctionEnum.rpad, new String[]{""});
        map.put(StringFunctionEnum.rtrim, new String[]{"RTRIM"});
        // map.put(StringFunctionEnum.split_part, new String[]{""});
        // map.put(StringFunctionEnum.strpos, new String[]{""});
        // map.put(StringFunctionEnum.substr, new String[]{""});
        // map.put(StringFunctionEnum.starts_with, new String[]{""});
        // map.put(StringFunctionEnum.to_ascii, new String[]{""});
        // map.put(StringFunctionEnum.to_hex, new String[]{""});
        map.put(StringFunctionEnum.translate, new String[]{"TRANSLATE"});   // SQL Server 2017 (14.x) and later
        // map.put(StringFunctionEnum.convert, new String[]{""});
        // map.put(StringFunctionEnum.convert_from, new String[]{""});
        // map.put(StringFunctionEnum.convert_to, new String[]{""});
        // map.put(StringFunctionEnum.encode, new String[]{""});
        // map.put(StringFunctionEnum.decode, new String[]{""});
        map.put(StringFunctionEnum.uuid, new String[]{"NEWID()"});


    }

    @Override
    public BasicDatabaseDao getDatabaseDao() {
        return this.databaseDao;
    }

    @Override
    public String getPaginationSQL(TableInfo ti, int offset, int limit) {

        if(ti.getPrimaryKeys().size() == 1){
            // 如果长度为1的话，自动识别ID
            // 是否考虑支持唯一键。
            String colName = ti.getPrimaryKeys().get(0).getColumnName();
            return "SELECT TOP " + limit + " * " +
                    "from (" +
                    "  select *, ROW_NUMBER() over( order by " + colName + " asc) as rowNumber" +
                    "   FROM " + this.getRightName(ti.getTableName()) +
                    ") a where rowNumber > " + offset;
        }

        // 无主键、多主键使用默认的分页查询方式
        return "SELECT * FROM " + this.getRightName(ti.getTableName());
    }

    @Override
    public void buildForeignKeys(TableInfo ti) {

    }

    @Override
    public void buildIndex(TableInfo ti) {

    }

    @Override
    protected void handleDataType(StringBuilder sqlBuilder, ColumnInfo columnInfo) {
        // timestamp -> binary
        // https://stackoverflow.com/questions/31377666/why-sql-server-timestamp-type-is-mapped-to-binary-type-in-hibernate

        SQLServerType sqlServerType = SQLServerType.getByJdbcType(columnInfo.getDataType());
        switch (sqlServerType) {

            case TIME:
            case DATETIME2:
            case DATETIMEOFFSET:
                //    time(7)
                //    datetime2(7)
                //    datetimeoffset(7)
                sqlBuilder.append(sqlServerType.getName());
                if(columnInfo.getDecimalDigits() > 0){
                    sqlBuilder.append("(").append(columnInfo.getDecimalDigits()).append(")");
                }

                break;

            case CHAR:
            case VARCHAR:
                if(columnInfo.getColumnSize() > SQLServerType.SHORT_VARTYPE_MAX_CHARS * 2) {
                    // 最多8000个英文，4000个汉字
                    // sqlServerType = SQLServerType.VARCHARMAX;
                    sqlBuilder.append(SQLServerType.VARCHARMAX.getName()).append("(max)");
                    break;
                }

                sqlBuilder.append(sqlServerType.getName()).append("(").append(columnInfo.getColumnSize()).append(")");
                break;

            case NCHAR:
            case NVARCHAR:
                if(columnInfo.getColumnSize() > SQLServerType.SHORT_VARTYPE_MAX_CHARS) {
                    // 可存储4000个字符，无论英文还是汉字
                    sqlBuilder.append(SQLServerType.NVARCHARMAX.getName()).append("(max)");
                    break;
                }

                sqlBuilder.append(sqlServerType.getName()).append("(").append(columnInfo.getColumnSize()).append(")");
                break;

            case BINARY:
            case VARBINARY:
                if(columnInfo.getColumnSize() > SQLServerType.SHORT_VARTYPE_MAX_BYTES) {
                    sqlBuilder.append(SQLServerType.VARBINARY.getName()).append("(max)");
                    break;
                }

                sqlBuilder.append(sqlServerType.getName()).append("(").append(columnInfo.getColumnSize()).append(")");
                break;

            case VARCHARMAX:
            case NVARCHARMAX:
            case VARBINARYMAX:
                sqlBuilder.append(sqlServerType.getName()).append("(max)");
                break;
            case DECIMAL:
            case NUMERIC:
                // 双精度
                sqlBuilder.append(sqlServerType.getName()).append("(").append(columnInfo.getColumnSize()).append(",").append(columnInfo.getDecimalDigits()).append(")");
                break;
            case UNKNOWN:
                switch (columnInfo.getDataType()){
                    case Types.CLOB:
                        // NVARCHARMAX
                        sqlBuilder.append(SQLServerType.NVARCHARMAX.getName()).append("(max)");
                        break;
                    case Types.BLOB:
                        // VARBINARYMAX
                        sqlBuilder.append(SQLServerType.VARBINARYMAX.getName()).append("(max)");
                        break;
                    default:
                        // other...
                        sqlBuilder.append(sqlServerType.getName());
                }
                break;
            case TINYINT:
            case BIT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case REAL:
            case SMALLDATETIME:
            case DATETIME:
            case DATE:

            case TEXT:
            case NTEXT:
            case IMAGE:

            case SMALLMONEY:
            case MONEY:

            case GUID:
            case SQL_VARIANT:
            case UDT:
            case XML:
            case TIMESTAMP:
            case GEOMETRY:
            case GEOGRAPHY:
                sqlBuilder.append(sqlServerType.getName());
        }
    }

    @Override
    public String getDatabaseVendor() {
        return DatabaseVendorEnum.SQLSERVER.getVendor();
    }

    @Override
    public String getRightName(String name) {
        if (this.getDatabaseConfig().isLowerCaseName()) {
            name = name.toLowerCase();
        }
        return String.format("[%s]", name);
    }



    // @Override
    // public Map<DateTypeEnum, String[]> getDateTypeDefaults() {
    //     return new HashMap<DateTypeEnum, String[]>(){
    //         {
    //             // https://docs.microsoft.com/en-us/sql/t-sql/functions/sysdatetime-transact-sql?view=sql-server-ver15
    //             //
    //             put(DateTypeEnum.DATE, new String[]{"getdate()", "getutcdate()"});
    //             put(DateTypeEnum.DATETIME, new String[]{"sysdatetime()", "sysdatetimeoffset()", "sysutcdatetime()"});
    //             put(DateTypeEnum.TIME, new String[]{"convert(time,sysdatetime())"});    // 无具体的函数
    //             put(DateTypeEnum.TIMESTAMP, new String[]{"current_timestamp"});
    //         }
    //     };
    // }


    @Override
    public String getAutoincrement() {
        return "IDENTITY(1, 1)";
    }

    @Override
    public void applySlaveDatabaseMetadata() {
        // 应用元数据信息

    }
}
