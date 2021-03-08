package com.liaobaikai.ngoxdb.core.service.impl.converter;

import com.liaobaikai.ngoxdb.bean.info.ColumnInfo;
import com.liaobaikai.ngoxdb.bean.info.ConstraintInfo;
import com.liaobaikai.ngoxdb.bean.info.IndexInfo2;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.bean.rs.ImportedKey;
import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
import com.liaobaikai.ngoxdb.core.config.DatabaseConfig;
import com.liaobaikai.ngoxdb.core.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.core.dao.DatabaseDao;
import com.liaobaikai.ngoxdb.core.dao.impl.OracleDatabaseDao;
import com.liaobaikai.ngoxdb.core.entity.SlaveMetaDataEntity;
import com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum;
import com.liaobaikai.ngoxdb.core.enums.fn.DatabaseFunctionEnum;
import com.liaobaikai.ngoxdb.core.enums.fn.DateTypeFunctionEnum;
import com.liaobaikai.ngoxdb.core.enums.fn.MathematicalFunctionEnum;
import com.liaobaikai.ngoxdb.core.enums.fn.StringFunctionEnum;
import com.liaobaikai.ngoxdb.core.service.DatabaseComparator;
import com.liaobaikai.ngoxdb.core.service.impl.comparator.OracleDatabaseComparator;
import com.liaobaikai.ngoxdb.utils.StringUtils;
import lombok.extern.slf4j.Slf4j;
import oracle.jdbc.OracleDatabaseMetaData;
import oracle.sql.TIMESTAMP;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Oracle数据库转换器
 * <p>
 * https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/sql-language-reference.pdf
 *
 * @author baikai.liao
 * @Time 2021-01-31 23:31:25
 */
@Slf4j
@Service
public class OracleDatabaseConverter extends BasicDatabaseConverter {

    // 标准的字节和扩展的字节
    private final static int STANDARD = 4000, EXTENDED = 32767;

    // nchar
    public final static int CHAR_MAX_CHARS = 1000;

    // char
    public final static int CHAR_MAX_BYTES = 2000;

    // nvarchar
    public final static int VARCHAR_MAX_CHARS = 2000;

    // varchar2
    public final static int VARCHAR_MAX_BYTES = 4000;

    // long max bytes
    // 2^31 - 1   =====  2G
    public final static int LONG_MAX_BYTES = 0x7FFFFFFF;

    /**
     * 2^32 - 1   =====  4G
     */
    public final static long BLOB_MAX_BYTES = 0xffffffff;


    private final OracleDatabaseDao databaseDao;
    private final OracleDatabaseComparator databaseComparator;

    public OracleDatabaseConverter() {
        this.databaseDao = null;
        this.databaseComparator = null;
    }

    public OracleDatabaseConverter(JdbcTemplate2 jdbcTemplate,
                                   boolean isMaster,
                                   String masterDatabaseVendor,
                                   DatabaseConfig databaseConfig) {
        super(jdbcTemplate, isMaster, masterDatabaseVendor, databaseConfig);
        this.databaseDao = new OracleDatabaseDao(jdbcTemplate);
        this.databaseComparator = new OracleDatabaseComparator(this);
        this.truncateMetadata();
    }

    @Override
    public Logger getLogger() {
        return log;
    }

    @Override
    protected void truncateMetadata() {
        if (!this.isMaster()) {
            if (this.getDatabaseDao().getTableCount(this.getDatabaseDao().getSchemaPattern(), SlaveMetaDataEntity.TABLE_NAME.toUpperCase()) == 0) {
                // 表不存在，先创建
                this.getDatabaseDao().createMetadataTable(this.getRightName(SlaveMetaDataEntity.TABLE_NAME));
            } else {
                // 清空表信息
                this.getDatabaseDao().truncateTable(this.getRightName(SlaveMetaDataEntity.TABLE_NAME));
            }
        }
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
    public boolean beforeCreateTable(TableInfo ti) {
        // 查询表是否存在
        String tName = this.getRightName(ti.getTableName());
        if (this.getDatabaseDao().getTableCount(this.getDatabaseDao().getSchemaPattern(), ti.getTableName().toUpperCase()) > 0) {
            if (this.getDatabaseConfig().isReplaceTable()) {
                getLogger().warn("[{}] Drop table if exists {}", this.getDatabaseConfig().getName(), tName);
                this.getDatabaseDao().dropTable(tName);
            } else {
                getLogger().warn("[{}] Table {} already exists", this.getDatabaseConfig().getName(), tName);
                return false;
            }
        }
        return true;
    }

    @Override
    public List<String> getUnsupportedFunctions() {
        return new ArrayList<String>() {
            {
                add("BITAND");
                add("COSH");
                add("NANVL");
                add("REMAINDER");
                add("SINH");
                add("TANH");

            }
        };
    }

    @Override
    public void buildDatabaseFunctionMap(Map<DatabaseFunctionEnum, String[]> map) {
        // https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/Functions.html#GUID-D079EFD3-C683-441F-977E-2C9503089982

        // 不支持
        // BITAND
        // COSH
        // NANVL
        // REMAINDER
        // SINH
        // TANH

        // 数学函数
        map.put(MathematicalFunctionEnum.abs, new String[]{"ABS"});
        // map.put(MathematicalFunctionEnum.cbrt, new String[]{""});
        map.put(MathematicalFunctionEnum.ceil, new String[]{"CEIL"});
        // map.put(MathematicalFunctionEnum.ceiling, new String[]{""});
        // map.put(MathematicalFunctionEnum.degrees, new String[]{""});
        // map.put(MathematicalFunctionEnum.div, new String[]{""});
        map.put(MathematicalFunctionEnum.exp, new String[]{"EXP"});
        // map.put(MathematicalFunctionEnum.factorial, new String[]{""});
        map.put(MathematicalFunctionEnum.floor, new String[]{"FLOOR"});
        // map.put(MathematicalFunctionEnum.gcd, new String[]{""});
        // map.put(MathematicalFunctionEnum.lcm, new String[]{""});
        map.put(MathematicalFunctionEnum.ln, new String[]{"LN"});
        map.put(MathematicalFunctionEnum.log, new String[]{"LOG"});
        // map.put(MathematicalFunctionEnum.log10, new String[]{""});
        // map.put(MathematicalFunctionEnum.min_scale, new String[]{""});
        map.put(MathematicalFunctionEnum.mod, new String[]{"MOD"});
        // map.put(MathematicalFunctionEnum.pi, new String[]{""});
        map.put(MathematicalFunctionEnum.power, new String[]{"POWER"});
        // map.put(MathematicalFunctionEnum.radians, new String[]{""});
        map.put(MathematicalFunctionEnum.round, new String[]{"ROUND"});
        // map.put(MathematicalFunctionEnum.scale, new String[]{""});
        map.put(MathematicalFunctionEnum.sign, new String[]{"SIGN"});
        map.put(MathematicalFunctionEnum.sqrt, new String[]{"SQRT"});
        // map.put(MathematicalFunctionEnum.trim_scale, new String[]{""});
        map.put(MathematicalFunctionEnum.trunc, new String[]{"TRUNC"});
        map.put(MathematicalFunctionEnum.width_bucket, new String[]{"WIDTH_BUCKET"});
        // map.put(MathematicalFunctionEnum.random, new String[]{""});
        map.put(MathematicalFunctionEnum.acos, new String[]{"ACOS"});
        // map.put(MathematicalFunctionEnum.acosd, new String[]{""});
        map.put(MathematicalFunctionEnum.asin, new String[]{"ASIN"});
        // map.put(MathematicalFunctionEnum.asind, new String[]{""});
        map.put(MathematicalFunctionEnum.atan, new String[]{"ATAN"});
        // map.put(MathematicalFunctionEnum.atand, new String[]{""});
        map.put(MathematicalFunctionEnum.atan2, new String[]{"ATAN2"});
        // map.put(MathematicalFunctionEnum.atan2d, new String[]{""});
        map.put(MathematicalFunctionEnum.cos, new String[]{"COS"});
        // map.put(MathematicalFunctionEnum.cosd, new String[]{""});
        // map.put(MathematicalFunctionEnum.cot, new String[]{""});
        // map.put(MathematicalFunctionEnum.cotd, new String[]{""});
        map.put(MathematicalFunctionEnum.sin, new String[]{"SIN"});
        // map.put(MathematicalFunctionEnum.sind, new String[]{""});
        map.put(MathematicalFunctionEnum.tan, new String[]{"TAN"});
        // map.put(MathematicalFunctionEnum.tand, new String[]{""});

        // 日期函数
        // 不支持
        // ADD_MONTHS
        // DBTIMEZONE
        // FROM_TZ
        // LAST_DAY
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

        map.put(DateTypeFunctionEnum.to_char, new String[]{"TO_CHAR"});
        // map.put(DateTypeFunctionEnum.to_date, new String[]{""});
        // map.put(DateTypeFunctionEnum.to_number, new String[]{""});
        map.put(DateTypeFunctionEnum.to_timestamp, new String[]{"TO_TIMESTAMP"});

        // map.put(DateTypeFunctionEnum.age, new String[]{""});
        map.put(DateTypeFunctionEnum.clock_timestamp, new String[]{"SYSDATE"});    // Current date and time (changes during statement execution);
        map.put(DateTypeFunctionEnum.current_date, new String[]{"CURRENT_DATE", "SYSDATE"});
        map.put(DateTypeFunctionEnum.current_time, new String[]{"SYSDATE"});
        map.put(DateTypeFunctionEnum.current_timestamp, new String[]{"CURRENT_TIMESTAMP"});
        // map.put(DateTypeFunctionEnum.date_part, new String[]{""});
        // map.put(DateTypeFunctionEnum.date_trunc, new String[]{""});
        map.put(DateTypeFunctionEnum.extract, new String[]{"EXTRACT"});
        // map.put(DateTypeFunctionEnum.isfinite, new String[]{""});
        // map.put(DateTypeFunctionEnum.justify_days, new String[]{""});
        // map.put(DateTypeFunctionEnum.justify_hours, new String[]{""});
        // map.put(DateTypeFunctionEnum.justify_interval, new String[]{""});
        map.put(DateTypeFunctionEnum.localtime, new String[]{"SYSDATE"});
        map.put(DateTypeFunctionEnum.localtimestamp, new String[]{"LOCALTIMESTAMP"});
        // map.put(DateTypeFunctionEnum.make_date, new String[]{""});
        // map.put(DateTypeFunctionEnum.make_interval, new String[]{""});
        // map.put(DateTypeFunctionEnum.make_time, new String[]{""});
        // map.put(DateTypeFunctionEnum.make_timestamp, new String[]{""});
        // map.put(DateTypeFunctionEnum.make_timestamptz, new String[]{""});
        map.put(DateTypeFunctionEnum.now, new String[]{"SYSDATE"});    // Current date and time (start of current transaction);
        map.put(DateTypeFunctionEnum.statement_timestamp, new String[]{"SYSTIMESTAMP"});    // Current date and time (start of current statement);
        map.put(DateTypeFunctionEnum.timeofday, new String[]{"SYSDATE"});
        map.put(DateTypeFunctionEnum.transaction_timestamp, new String[]{"SYSTIMESTAMP"});  // Current date and time (start of current transaction);

        // 字符函数
        // 不支持
        // NLSSORT
        // REGEXP_REPLACE
        // REGEXP_SUBSTR
        // SOUNDEX
        // TRANSLATE ... USING
        // INSTR
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

        // map.put(StringFunctionEnum.bit_length, new String[]{""});
        // map.put(StringFunctionEnum.char_length, new String[]{""});
        // map.put(StringFunctionEnum.character_length, new String[]{""});
        map.put(StringFunctionEnum.lower, new String[]{"LOWER", "NLS_LOWER"});
        // map.put(StringFunctionEnum.normalize, new String[]{""});
        // map.put(StringFunctionEnum.octet_length, new String[]{""});
        // map.put(StringFunctionEnum.overlay, new String[]{""});
        // map.put(StringFunctionEnum.position, new String[]{""});
        // map.put(StringFunctionEnum.substring, new String[]{""});
        map.put(StringFunctionEnum.trim, new String[]{"TRIM"});
        map.put(StringFunctionEnum.upper, new String[]{"NLS_UPPER", "UPPER"});
        map.put(StringFunctionEnum.ascii, new String[]{"ASCII"});
        // map.put(StringFunctionEnum.btrim, new String[]{""});
        map.put(StringFunctionEnum.chr, new String[]{"CHR", "NCHR"});
        map.put(StringFunctionEnum.concat, new String[]{"CONCAT"});
        // map.put(StringFunctionEnum.concat_ws, new String[]{""});
        // map.put(StringFunctionEnum.format, new String[]{""});
        map.put(StringFunctionEnum.initcap, new String[]{"INITCAP", "NLS_INITCAP"});
        // map.put(StringFunctionEnum.left, new String[]{""});
        map.put(StringFunctionEnum.length, new String[]{"LENGTH"});
        map.put(StringFunctionEnum.lpad, new String[]{"LPAD"});
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
        // map.put(StringFunctionEnum.repeat, new String[]{""});
        map.put(StringFunctionEnum.replace, new String[]{"REPLACE"});
        // map.put(StringFunctionEnum.reverse, new String[]{""});
        // map.put(StringFunctionEnum.right, new String[]{""});
        map.put(StringFunctionEnum.rpad, new String[]{"RPAD"});
        map.put(StringFunctionEnum.rtrim, new String[]{"RTRIM"});
        // map.put(StringFunctionEnum.split_part, new String[]{""});
        // map.put(StringFunctionEnum.strpos, new String[]{""});
        map.put(StringFunctionEnum.substr, new String[]{"SUBSTR"});
        // map.put(StringFunctionEnum.starts_with, new String[]{""});
        map.put(StringFunctionEnum.to_ascii, new String[]{"ASCII"});
        // map.put(StringFunctionEnum.to_hex, new String[]{""});
        map.put(StringFunctionEnum.translate, new String[]{"TRANSLATE"});
        // map.put(StringFunctionEnum.convert, new String[]{""});
        // map.put(StringFunctionEnum.convert_from, new String[]{""});
        // map.put(StringFunctionEnum.convert_to, new String[]{""});
        // map.put(StringFunctionEnum.encode, new String[]{""});
        // map.put(StringFunctionEnum.decode, new String[]{""});

        // https://stackoverflow.com/questions/13951576/how-to-generate-a-version-4-random-uuid-on-oracle
        map.put(StringFunctionEnum.uuid, new String[]{"SYS_GUID()"});

    }

    @Override
    public BasicDatabaseDao getDatabaseDao() {
        return this.databaseDao;
    }

    @Override
    public String getPaginationSQL(TableInfo ti, int offset, int limit) {
        // https://zhuanlan.zhihu.com/p/59286113
        // 可选：order by ROWID

        // 唯一键
        String colNames = this.getTableUniqueKeys(ti.getUniqueKeys());
        if (colNames.length() == 0) {
            // 无唯一键
            // 使用全部列
            colNames = this.getTableOrderColumnNames(ti.getColumns());
        }

        return String.format("SELECT " + this.getTableColumnNames(ti.getColumns(), "T0") + " FROM (" +
                        "SELECT ROWNUM %s, T.* FROM (SELECT * FROM %s ORDER BY %s) T WHERE ROWNUM <= %s" +
                        ") T0 WHERE T0.%s > %s",
                this.getRightName("$ROW_NUM$"),
                this.getRightName(ti.getTableName()),
                colNames,
                offset + limit,
                this.getRightName("$ROW_NUM$"),
                offset);
    }

    @Override
    protected void handleDataType(StringBuilder sqlBuilder, ColumnInfo columnInfo) {
        // https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlqr/Data-Types.html#GUID-219C338B-FE60-422A-B196-2F0A01CAD9A4
        // Table 6-2 ANSI Data Types Converted to Oracle Data Types

        // https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT1832
        // https://www.mat.unical.it/~rullo/teaching/basidati_ev/aa09-10/materialedidatticolab/Oracle%20Datatypes.pdf

        // 16383 if MAX_STRING_SIZE = EXTENDED and the national character set is AL16UTF16
        // 32767 if MAX_STRING_SIZE = EXTENDED and the national character set is UTF8
        // 2000 if MAX_STRING_SIZE = STANDARD and the national character set is AL16UTF16
        // 4000 if MAX_STRING_SIZE = STANDARD and the national character set is UTF8
        String typeName = "";   // 类型名称
        int length = -1;        // 字符长度
        int precision = -1;     // 精度
        int scale = -1;         // 刻度

        int chars = columnInfo.getColumnSize();

        // https://docs.oracle.com/cd/B19306_01/gateways.102/b14270/apa.htm
        switch (columnInfo.getDataType()) {
            case Types.BOOLEAN:
            case Types.BIT:
                // https://stackoverflow.com/questions/2426145/oracles-lack-of-a-bit-datatype-for-table-columns
                typeName = "NUMBER";
                precision = 1;
                break;
            case Types.TINYINT:
                typeName = "NUMBER";
                precision = 3;
                break;
            case Types.SMALLINT:
                typeName = "NUMBER";
                precision = 5;
                break;
            case Types.INTEGER:
                typeName = "NUMBER";
                precision = 10;
                break;
            case Types.BIGINT:
                typeName = "NUMBER";
                precision = 19;
                break;
            case Types.FLOAT:
                typeName = "FLOAT";
                precision = 49;
                break;
            case Types.DOUBLE:
                typeName = "FLOAT";
                precision = 126;
                break;
            case Types.REAL:
                typeName = "FLOAT";
                precision = 23;
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                typeName = "NUMERIC";

                // 1 ~ 38
                precision = Math.min(columnInfo.getColumnSize(), 38);
                // -84 ~ 127
                scale = Math.min(columnInfo.getDecimalDigits(), 127);
                scale = Math.max(scale, -84);
                break;
            case Types.CHAR:
                // 字节大小
                if (chars > VARCHAR_MAX_CHARS) {
                    // bytes is (4000, ∞)
                    typeName = this.isSameDatabaseVendor() ? "CLOB" : "NCLOB";
                    break;
                } else if (CHAR_MAX_CHARS < chars) {
                    // bytes is (2000, 3999]
                    typeName = this.isSameDatabaseVendor() ? "VARCHAR2" : "NVARCHAR2";
                } else {
                    // bytes is (-∞, 2000]
                    typeName = this.isSameDatabaseVendor() ? "CHAR" : "NCHAR";
                }
                length = chars;
                break;
            case Types.VARCHAR:
                if (chars > VARCHAR_MAX_CHARS) {
                    // bytes is (4000, ∞)
                    typeName = this.isSameDatabaseVendor() ? "CLOB" : "NCLOB";
                    break;
                } else {
                    // bytes is (-∞, 4000]
                    typeName = this.isSameDatabaseVendor() ? "VARCHAR2" : "NVARCHAR2";
                }
                length = chars;
                break;
            case Types.CLOB:
            case Types.LONGVARCHAR:
                typeName = "CLOB";
                break;
            case Types.DATE:
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
            case microsoft.sql.Types.DATETIME:
            case microsoft.sql.Types.SMALLDATETIME:
            case microsoft.sql.Types.DATETIMEOFFSET:
                typeName = "DATE";
                break;
            case Types.TIMESTAMP:
                typeName = "TIMESTAMP";
                break;
            case Types.BINARY:
            case Types.VARBINARY:
                if (columnInfo.getColumnSize() > VARCHAR_MAX_CHARS) {
                    // (2000, 2GB]
                    // https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/Data-Types.html#GUID-8EFA29E9-E8D8-40A6-A43E-954908C954A4
                    // If MAX_STRING_SIZE = STANDARD, then the size limits for releases prior to Oracle Database 12c apply: 4000 bytes
                    // for the VARCHAR2 and NVARCHAR2 data types, and 2000 bytes for the RAW data type. This is the default.
                    typeName = "LONG RAW";
                    break;
                } else {
                    // (-∞, 2000]
                    typeName = "RAW";
                }
                length = columnInfo.getColumnSize();
                break;
            case Types.BLOB:
            case Types.LONGVARBINARY:
                typeName = "BLOB";  // LONG RAW
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
            // case Types.ROWID:
            //     break;
            case Types.NCHAR:
                if (chars > VARCHAR_MAX_CHARS) {
                    // chars is (2000, ∞)
                    typeName = "NCLOB";
                    break;
                } else if (CHAR_MAX_CHARS < chars) {
                    // chars is (1000, 1999]
                    typeName = "NVARCHAR2";
                } else {
                    // chars is (-∞, 1000]
                    typeName = "NCHAR";
                }
                length = chars;
                break;
            case Types.NVARCHAR:
                if (chars > VARCHAR_MAX_CHARS) {
                    // chars is (2000, ∞)
                    typeName = "NCLOB";
                    break;
                } else {
                    // chars is (-∞, 2000]
                    typeName = "NVARCHAR2";
                }
                length = chars;
                break;
            case Types.LONGNVARCHAR:
            case Types.NCLOB:
            case Types.SQLXML:
                typeName = "NCLOB";
                break;
            // case Types.REF_CURSOR:
            //     break;
            case Types.TIMESTAMP_WITH_TIMEZONE:
                typeName = "TIMESTAMP WITH TIME ZONE";
                break;
            case microsoft.sql.Types.GEOMETRY:
                typeName = "GEOMETRY";
                break;
            case microsoft.sql.Types.GEOGRAPHY:
                typeName = "GEOGRAPHY";
                break;
            case microsoft.sql.Types.SQL_VARIANT:
                typeName = "VARBINARY";
                length = 8000;
                break;
            case microsoft.sql.Types.GUID:
                // UNIQUEIDENTIFIER
                typeName = "CHAR";
                length = 36;
                break;
            case microsoft.sql.Types.MONEY:
                typeName = "NUMBER";
                precision = 19;
                scale = 4;
                break;
            case microsoft.sql.Types.SMALLMONEY:
                typeName = "NUMBER";
                precision = 10;
                scale = 4;
                break;
        }

        sqlBuilder.append(typeName);
        if (length != -1) {
            sqlBuilder.append("(").append(length).append(")");
        } else if (precision != -1) {
            // 数字精度、刻度
            sqlBuilder.append("(").append(precision);
            if (scale != -1) {
                sqlBuilder.append(", ").append(scale);
            }
            sqlBuilder.append(")");
        }

    }

    @Override
    public String getDatabaseVendor() {
        return DatabaseVendorEnum.ORACLE.getVendor();
    }

    @Override
    public String getRightName(String name) {
        return "\"" + name.toUpperCase() + "\"";
    }

    @Override
    public void formatConstraintColumnName(ConstraintInfo ci, List<ColumnInfo> columnInfoList) {
        String checkCondition = ci.getCheckCondition();
        // 去掉特有的格式
        for (ColumnInfo columnInfo : columnInfoList) {
            checkCondition = checkCondition.replaceAll(
                    StringUtils.escape("\"" + columnInfo.getColumnName() + "\""), columnInfo.getColumnName());
        }
        ci.setCheckCondition(checkCondition);
    }

    @Override
    public String buildCreateTable(TableInfo ti) {
        // https://docs.oracle.com/en/database/oracle/oracle-database/12.2/sqlrf/CREATE-TABLE.html#GUID-F9CE0CC3-13AE-4744-A43C-EAC7A71AAAB6
        // default on
        String table = this.getRightName(ti.getTableName());
        StringBuilder sqlBuilder = new StringBuilder(
                "CREATE TABLE " + table + " ( "
        );

        // 列信息
        ti.getColumns().forEach(columnInfo -> {

            final String cName = getRightName(columnInfo.getColumnName());

            sqlBuilder.append(cName).append(" ");

            if (this.isSameDatabaseVendor()) {
                // 相同的数据库厂家
                sqlBuilder.append(columnInfo.getColumnType());
            } else {
                // 其他数据库厂家
                this.handleDataType(sqlBuilder, columnInfo);
            }

            sqlBuilder.append(" ");

            // 默认值
            if (columnInfo.getColumnDef() != null && !"NULL".equalsIgnoreCase(columnInfo.getColumnDef())) {
                // 默认值
                String dataTypeDef = this.getDatabaseDataTypeDefault(columnInfo.getColumnDef(), columnInfo.getDataType());
                // 时间固定默认值，如：1970-01-01 01:01:01
                // ORA-01847: 月份中日的值必须介于 1 和当月最后一日之间
                // oracle需要将字符串转为时间保存。
                if (ArrayUtils.contains(DatabaseDao.DATE_TIME_JDBC_TYPES, columnInfo.getDataType()) && dataTypeDef == null) {
                    // 时间格式
                    // 应该是字符串类型的固定时间了
                    log.info("表{}，列: {}，默认值: {}，不支持自动创建，需人工创建！", table, cName, columnInfo.getColumnDef());
                    // sqlBuilder.append("DEFAULT ").append(columnInfo.getColumnDef()).append(" ");
                } else {
                    sqlBuilder.append("DEFAULT ").append(dataTypeDef == null ? columnInfo.getColumnDef() : dataTypeDef).append(" ");
                }

            }

            if (columnInfo.isNotNull()) {
                sqlBuilder.append("NOT NULL ");
            }

            sqlBuilder.append(",");

        });

        // 主键
        buildPrimaryKeys(ti, sqlBuilder);
        // 约束
        buildConstraintInfo(ti, sqlBuilder);

        sqlBuilder.delete(sqlBuilder.length() - 1, sqlBuilder.length()).append(") ");

        return sqlBuilder.toString();
    }

    @Override
    protected void buildForeignKeyChangeRules(StringBuilder sBuilder, ImportedKey importedKey) {

        // https://docs.oracle.com/cd/E11882_01/server.112/e41084/statements_3001.htm#CJAHHIBI
        // https://docs.oracle.com/cd/E11882_01/server.112/e41084/clauses002.htm#CJAFFBAA

        // oracle仅支持以下属性
        // [ [ [ NOT ] DEFERRABLE ]
        //   [ INITIALLY { IMMEDIATE | DEFERRED } ]
        // | [ INITIALLY { IMMEDIATE | DEFERRED } ]
        //   [ [ NOT ] DEFERRABLE ]
        // ]
        // [ RELY | NORELY ]
        // [ using_index_clause ]
        // [ ENABLE | DISABLE ]
        // [ VALIDATE | NOVALIDATE ]
        // [ exceptions_clause ]
    }

    @Override
    protected String buildIndexName(IndexInfo2 ii, TableInfo ti) {
        String identifier = super.buildIndexName(ii, ti);
        int maxIdentifierLength = this.getDatabaseDao().getMaxColumnNameLength();
        if(identifier.length() > maxIdentifierLength) {
            // java.sql.SQLSyntaxErrorException: ORA-00972: 标识符过长
            // ORA-00972: Identifier is too Long (Doc ID 1955166.1)
            // In an Oracle database, objects names have a maximum of 30 characters.
            // This is why the ORA-00972 error is returned: the generated Supplemental Log Group name (identifier) is too long.
            if (ii.isNonUnique()) {
                identifier = "idx_";
            } else {
                identifier = "ux_";
            }

            if (identifier.length() + ti.getTableName().length() <= maxIdentifierLength) {
                identifier += ti.getTableName() + "_";
            }

            identifier += UUID.randomUUID().toString().replaceAll("-", "").substring(0, maxIdentifierLength - identifier.length());
        }
        return identifier;
    }

    @Override
    public String getDatabaseDataTypeDefault(String masterDataTypeDef, int dataType) {
        String defaultValue = super.getDatabaseDataTypeDefault(masterDataTypeDef, dataType);
        if (dataType == Types.BOOLEAN) {
            if ("true".equals(masterDataTypeDef)) {
                return "1";
            } else if ("false".equals(masterDataTypeDef)) {
                return "0";
            }
        }
        return defaultValue;
    }

    @Override
    public void changeBatchArgs(List<Object[]> batchArgs, TableInfo ti) {

        // oracle的空值和NULL同一个意思。因此添加not null约束的时候，'' 和 NULL均会抛出 ora-01400
        // 因此，对于not null的字段，且传入的值是 ''，需要替换成 ' '，NULL的值需要替换为默认值。
        for (Object[] objects : batchArgs) {
            // row
            for (int x = 0, len = objects.length; x < len; x++) {
                // cell
                ColumnInfo columnInfo = ti.getColumns().get(x);
                if (!columnInfo.isNotNull()) {
                    // enable null
                    continue;
                }
                // 下面的类型需要判断。
                switch (columnInfo.getDataType()) {
                    case Types.VARCHAR:
                    case Types.NVARCHAR:

                    case Types.CHAR:
                    case Types.NCHAR:

                    case Types.CLOB:
                    case Types.NCLOB:

                    case Types.LONGVARCHAR:
                    case Types.LONGNVARCHAR:
                        if (objects[x] == null) {
                            // 使用默认值
                            objects[x] = columnInfo.getColumnDef();
                        } else if ("".equals(objects[x])) {
                            // 空字符串
                            objects[x] = " ";
                        }
                        break;
                }

            }
        }
    }

    @Override
    public Object convertData(Object srcObj) throws SQLException {

        if (srcObj instanceof oracle.sql.TIMESTAMP) {
            // oracle.sql.TIMESTAMPLTZ 暂不支持
            return ((TIMESTAMP) srcObj).timestampValue();
        } else {
            // ...
        }

        return super.convertData(srcObj);
    }

    @Override
    public DatabaseComparator getComparator() {
        return this.databaseComparator;
    }



}
