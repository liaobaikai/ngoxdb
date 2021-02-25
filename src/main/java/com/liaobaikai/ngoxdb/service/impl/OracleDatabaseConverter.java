package com.liaobaikai.ngoxdb.service.impl;

import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
import com.liaobaikai.ngoxdb.config.DatabaseConfig;
import com.liaobaikai.ngoxdb.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.dao.DatabaseDao;
import com.liaobaikai.ngoxdb.dao.impl.OracleDatabaseDao;
import com.liaobaikai.ngoxdb.entity.SlaveMetaDataEntity;
import com.liaobaikai.ngoxdb.enums.DatabaseVendorEnum;
import com.liaobaikai.ngoxdb.enums.fn.DatabaseFunctionEnum;
import com.liaobaikai.ngoxdb.enums.fn.DateTypeFunctionEnum;
import com.liaobaikai.ngoxdb.enums.fn.MathematicalFunctionEnum;
import com.liaobaikai.ngoxdb.enums.fn.StringFunctionEnum;
import com.liaobaikai.ngoxdb.info.ColumnInfo;
import com.liaobaikai.ngoxdb.info.ConstraintInfo;
import com.liaobaikai.ngoxdb.info.TableInfo;
import com.liaobaikai.ngoxdb.service.BasicDatabaseConverter;
import com.liaobaikai.ngoxdb.utils.StringUtils;
import lombok.extern.slf4j.Slf4j;
import oracle.jdbc.OracleType;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Oracle数据库转换器
 *
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

    public OracleDatabaseConverter() {
        this.databaseDao = null;
    }

    public OracleDatabaseConverter(JdbcTemplate2 jdbcTemplate,
                                   boolean isMaster,
                                   String masterDatabaseVendor,
                                   DatabaseConfig databaseConfig) {
        super(jdbcTemplate, isMaster, masterDatabaseVendor, databaseConfig);
        this.databaseDao = new OracleDatabaseDao(jdbcTemplate);
        this.truncateMetadata();
    }

    @Override
    protected void truncateMetadata() {
        if(!this.isMaster()){
            if(this.getDatabaseDao().getTableCount(this.getDatabaseDao().getSchemaPattern(), SlaveMetaDataEntity.TABLE_NAME.toUpperCase()) == 0) {
                // 表不存在，先创建
                this.getDatabaseDao().createMetadataTable(SlaveMetaDataEntity.TABLE_NAME);
            } else {
                // 清空表信息
                this.getDatabaseDao().truncateTable(SlaveMetaDataEntity.TABLE_NAME);
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
        DatabaseConfig databaseConfig = this.getDatabaseConfig();
        if(databaseConfig.isReplaceTable()){
            // 替换表
            // 查询表是否存在
            final String table = ti.getTableName().toUpperCase();
            if(this.getDatabaseDao().getTableCount(this.getDatabaseDao().getSchemaPattern(), table) > 0){
                log.info("表{}已存在, 即将删除重建...", table);
                this.getDatabaseDao().dropTable(table);
            }
        }
        return true;
    }

    @Override
    public List<String> getUnsupportedFunctions() {
        return new ArrayList<String>(){
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
        return String.format("SELECT * FROM (" +
                        "SELECT ROWNUM RN, T.* FROM (SELECT * FROM %s ORDER BY ROWID) T WHERE ROWNUM <= %s" +
                        ") T0 WHERE T0.RN > %s",
                ti.getTableName(), offset + limit, offset);
    }

    @Override
    public void buildForeignKeys(TableInfo ti) {

    }

    @Override
    public void buildIndex(TableInfo ti) {

    }

    @Override
    protected void handleDataType(StringBuilder sqlBuilder, ColumnInfo columnInfo) {
        // https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlqr/Data-Types.html#GUID-219C338B-FE60-422A-B196-2F0A01CAD9A4

        // https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT1832
        // https://www.mat.unical.it/~rullo/teaching/basidati_ev/aa09-10/materialedidatticolab/Oracle%20Datatypes.pdf

        // 16383 if MAX_STRING_SIZE = EXTENDED and the national character set is AL16UTF16
        // 32767 if MAX_STRING_SIZE = EXTENDED and the national character set is UTF8
        // 2000 if MAX_STRING_SIZE = STANDARD and the national character set is AL16UTF16
        // 4000 if MAX_STRING_SIZE = STANDARD and the national character set is UTF8

        // 通过jdbcType获取oracle的类型
        OracleType oracleType;
        try {
            oracleType = OracleType.toOracleType(columnInfo.getDataType());
        } catch (SQLException e) {
            throw new NullPointerException("sql.Types[" + columnInfo.getDataType() + "]无法转换为OracleType");
        }

        // 数据库转换过来的目标类型
        if(oracleType == null){
            // 转换失败
            switch (columnInfo.getDataType()){
                case Types.FLOAT:
                case Types.REAL:
                case Types.DOUBLE:
                    oracleType = OracleType.FLOAT;
                    break;

                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                case Types.BIGINT:
                case Types.NUMERIC:
                case Types.DECIMAL:
                    oracleType = OracleType.NUMBER;
                    break;
                case Types.BIT:
                case Types.BOOLEAN:
                case Types.CHAR:
                    oracleType = OracleType.CHAR;
                    break;
                case Types.VARCHAR:
                    oracleType = OracleType.VARCHAR2;
                    break;
                case Types.LONGVARCHAR:
                case Types.CLOB:
                    oracleType = OracleType.CLOB;
                    break;
                case Types.DATE:
                case Types.TIME:
                    oracleType = OracleType.DATE;
                    break;
                case Types.TIMESTAMP:
                    oracleType = OracleType.TIMESTAMP;
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                case Types.BLOB:
                    oracleType = OracleType.BLOB;
                    break;

                case Types.ARRAY:
                    // 数组
                    oracleType = OracleType.VARRAY;
                    break;
                case Types.REF:
                    oracleType = OracleType.REF;
                    break;
                case Types.DATALINK:
                    break;
                case Types.ROWID:
                    oracleType = OracleType.ROWID;
                    break;
                case Types.NCHAR:
                    oracleType = OracleType.NCHAR;
                    break;
                case Types.NVARCHAR:
                    oracleType = OracleType.NVARCHAR;
                    break;
                case Types.LONGNVARCHAR:
                case Types.NCLOB:
                    oracleType = OracleType.NCLOB;
                    break;

                case Types.NULL:
                case Types.OTHER:
                case Types.JAVA_OBJECT:
                case Types.DISTINCT:
                case Types.STRUCT:
                case Types.SQLXML:
                case Types.REF_CURSOR:
                    oracleType = null;
                    break;
                case Types.TIME_WITH_TIMEZONE:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    oracleType = OracleType.TIMESTAMP_WITH_TIME_ZONE;
                    break;
            }
        }

        if(oracleType == null){
            return;
        }

        int chars = columnInfo.getColumnSize();
        // number is zero/0
        int bytes = columnInfo.getCharOctetLength();

        switch (oracleType) {

            case VARCHAR2:
                // 以字节为单位
                // 占用1个字节：ascii
                // 2/3个字节：中文
                // 4个字节：中文
                // https://blog.csdn.net/yidian815/article/details/12999251
                // https://stackoverflow.com/questions/18325015/difference-between-nvarchar-in-oracle-and-sql-server
                if(chars != bytes){
                    // 数据库以字符方式计算。
                    bytes = chars * this.getCharBytes();
                }

                if (bytes > VARCHAR_MAX_BYTES) {
                    // bytes > 4000
                    // long is deprecated, replace to CLOB or NCLOB
                    sqlBuilder.append(OracleType.CLOB.getName());
                } else {
                    sqlBuilder.append(oracleType.getName()).append("(").append(bytes).append(")");
                }
                break;

            case NVARCHAR:
                // https://www.oracletutorial.com/oracle-basics/oracle-nvarchar2/
                // 以字符为单位
                // https://www.cnblogs.com/flyingfish/archive/2010/01/15/1648448.html
                if (chars > VARCHAR_MAX_CHARS) {
                    // chars > 2000
                    // long is deprecated, replace to CLOB or NCLOB
                    sqlBuilder.append(OracleType.NCLOB.getName());
                } else {
                    sqlBuilder.append("NVARCHAR2").append("(").append(chars).append(")");
                }

                break;

            case CHAR:
                // char 以字节为单位 默认为1
                if(chars != bytes){
                    // 数据库以字节方式计算。
                    bytes = chars * this.getCharBytes();
                }

                if(bytes > VARCHAR_MAX_BYTES){
                    // bytes is (4000, ∞)
                    sqlBuilder.append(OracleType.CLOB.getName());
                    break;
                } else if(CHAR_MAX_BYTES < bytes){
                    // bytes is (2000, 3999]
                    sqlBuilder.append(OracleType.VARCHAR2.getName());
                } else {
                    // bytes is (-∞, 2000]
                    sqlBuilder.append(oracleType.getName());
                }

                sqlBuilder.append("(").append(chars).append(")");
                break;
            case NCHAR:
                // nchar 以字符为单位
                if(chars > VARCHAR_MAX_CHARS){
                    // chars is (2000, ∞)
                    sqlBuilder.append(OracleType.NCLOB.getName());
                    break;
                } else if(CHAR_MAX_CHARS < chars){
                    // chars is (1000, 1999]
                    sqlBuilder.append(OracleType.VARCHAR2.getName());
                } else {
                    // chars is (-∞, 1000]
                    sqlBuilder.append(oracleType.getName());
                }

                sqlBuilder.append("(").append(chars).append(")");
                break;
            case NUMBER:
                // https://stackoverflow.com/questions/13494010/difference-between-number-and-integer-datatype-in-oracle-dictionary-views

                // Number最多38个有效数字，
                // set DECIMAL(40, 0)
                // ORA-01727: numeric precision specifier is out of range (1 to 38)
                // https://docs.oracle.com/cd/B19306_01/olap.102/b14346/dml_datatypes002.htm
                // oracle                   jdbc                sql.types
                // number               number(0, -127)             2
                // number(38,0)         number(38, 0)               2
                // number(38)           number(38, 0)               2
                // decimal(38, 38)      number(38, 38)              2
                // decimal(38, 40)      number(38, 40)              2
                // integer              number(38, 0)               2
                // smallint             number(38, 0)               2

                switch (columnInfo.getDecimalDigits()){
                    case 0:
                        sqlBuilder.append(oracleType.getName());
                        if(columnInfo.getColumnSize() > 0){
                            sqlBuilder.append("(").append(columnInfo.getColumnSize()).append(")");
                        }
                        break;
                    case -127:
                        sqlBuilder.append(oracleType.getName());
                        if(columnInfo.getColumnSize() > 0){
                            sqlBuilder.append("(")
                                    .append(columnInfo.getColumnSize()).append(",")
                                    .append(columnInfo.getDecimalDigits()).append(")");
                        }
                        break;
                    default:
                        sqlBuilder.append(oracleType.getName())
                                .append("(").append(columnInfo.getColumnSize()).append(",")
                                .append(columnInfo.getDecimalDigits()).append(")");
                        break;
                }

                break;

            case RAW:
                // binary
                if(bytes > 2000){
                    // (2000, 2GB]
                    // https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/Data-Types.html#GUID-8EFA29E9-E8D8-40A6-A43E-954908C954A4
                    // If MAX_STRING_SIZE = STANDARD, then the size limits for releases prior to Oracle Database 12c apply: 4000 bytes
                    // for the VARCHAR2 and NVARCHAR2 data types, and 2000 bytes for the RAW data type. This is the default.
                    sqlBuilder.append(OracleType.BLOB.getName());
                } else {
                    // (-∞, 2000]
                    sqlBuilder.append(OracleType.RAW.getName()).append("(").append(bytes).append(")");
                }
                break;

            case LONG_RAW:
                // long binary
                sqlBuilder.append(OracleType.BLOB.getName());
                break;
            case LONG:
                // https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT413
                // https://docs.oracle.com/cd/E11882_01/appdev.112/e10646/oci03typ.htm#LNOCI16266
                // 2^31-1 bytes (2 gigabytes)
                sqlBuilder.append(OracleType.CLOB.getName());
                break;
            case FLOAT:
            case DATE:
            case BINARY_FLOAT:
            case BINARY_DOUBLE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case INTERVAL_YEAR_TO_MONTH:
            case INTERVAL_DAY_TO_SECOND:
            case PLSQL_BOOLEAN:

            case ROWID:
            case UROWID:
            case CLOB:
            case NCLOB:
            case BLOB:
            case BFILE:
            case JSON:
            case OBJECT:
            case REF:
            case VARRAY:
            case NESTED_TABLE:
            case ANYTYPE:
            case ANYDATA:
            case ANYDATASET:
            case XMLTYPE:
            case HTTPURITYPE:
            case XDBURITYPE:
            case DBURITYPE:
            case SDO_GEOMETRY:
            case SDO_TOPO_GEOMETRY:
            case SDO_GEORASTER:
            case ORDAUDIO:
            case ORDDICOM:
            case ORDDOC:
            case ORDIMAGE:
            case ORDVIDEO:
            case SI_AVERAGE_COLOR:
            case SI_COLOR:
            case SI_COLOR_HISTOGRAM:
            case SI_FEATURE_LIST:
            case SI_POSITIONAL_COLOR:
            case SI_STILL_IMAGE:
            case SI_TEXTURE:

            default:
                // 无需设置宽度
                sqlBuilder.append(oracleType.getName());
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
    public StringBuilder buildCreateTable(TableInfo ti) {
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
            if (columnInfo.getColumnDef() != null) {
                // 默认值
                String dataTypeDef = this.getDatabaseDataTypeDefault(columnInfo.getColumnDef(), columnInfo.getDataType());
                // 时间固定默认值，如：1970-01-01 01:01:01
                // ORA-01847: 月份中日的值必须介于 1 和当月最后一日之间
                // oracle需要将字符串转为时间保存。
                if(ArrayUtils.contains(DatabaseDao.DATE_TIME_JDBC_TYPES, columnInfo.getDataType()) && dataTypeDef == null){
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

        return sqlBuilder;
    }

    @Override
    protected void buildComment(TableInfo ti) {
        // https://docs.oracle.com/en/database/oracle/oracle-database/12.2/sqlrf/COMMENT.html#GUID-65F447C4-6914-4823-9691-F15D52DB74D7
        // To add a comment to a table, view, or materialized view, you must have COMMENT ANY TABLE system privilege.
        final String tName = ti.getTableName();

        // 列注释
        ti.getColumns().forEach(c -> {
            if(StringUtils.isEmpty(c.getRemarks())){
                return;
            }

            String s = String.format("COMMENT ON COLUMN %s.%s IS '%s'",
                    this.getRightName(tName),
                    this.getRightName(c.getColumnName()),
                    c.getRemarks());
            this.getDatabaseDao().insertMetadata(new SlaveMetaDataEntity(tName, SlaveMetaDataEntity.TYPE_COMMENT, s));

        });

        // 表注释
        if(StringUtils.isNotEmpty(ti.getRemarks())){

            String s = String.format("COMMENT ON TABLE %s IS '%s'",
                    this.getRightName(tName),
                    ti.getRemarks());
            this.getDatabaseDao().insertMetadata(new SlaveMetaDataEntity(tName, SlaveMetaDataEntity.TYPE_COMMENT, s));
        }

    }

}
