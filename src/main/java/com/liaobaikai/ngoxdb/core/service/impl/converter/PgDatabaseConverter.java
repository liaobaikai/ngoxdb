package com.liaobaikai.ngoxdb.core.service.impl.converter;

import com.liaobaikai.ngoxdb.bean.info.ColumnInfo;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
import com.liaobaikai.ngoxdb.core.config.DatabaseConfig;
import com.liaobaikai.ngoxdb.core.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.core.dao.impl.PgDatabaseDao;
import com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum;
import com.liaobaikai.ngoxdb.core.enums.fn.DatabaseFunctionEnum;
import com.liaobaikai.ngoxdb.core.enums.fn.DateTypeFunctionEnum;
import com.liaobaikai.ngoxdb.core.enums.fn.MathematicalFunctionEnum;
import com.liaobaikai.ngoxdb.core.enums.fn.StringFunctionEnum;
import com.liaobaikai.ngoxdb.core.service.DatabaseComparator;
import com.liaobaikai.ngoxdb.core.service.impl.comparator.PgDatabaseComparator;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.core.Oid;
import org.postgresql.jdbc.TypeInfoCache;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * PostgreSQL数据库转换器
 *
 * @author baikai.liao
 * @Time 2021-02-02 09:00:40
 */
@Slf4j
@Service
public class PgDatabaseConverter extends BasicDatabaseConverter {

    private final PgDatabaseDao databaseDao;
    private final TypeInfoCache typeInfoCache;
    private final PgDatabaseComparator databaseComparator;
    private UUIDStatus uuidStatus;

    /**
     * UUID状态
     */
    enum UUIDStatus {
        ENABLED,        // 已启用
        NOT_EXISTS,     // 不存在，需执行：CREATE EXTENSION pgcrypto;
        NO_PERMISSION   // 无权限，已执行过：CREATE EXTENSION pgcrypto，但执行失败！
    }

    public PgDatabaseConverter() {
        this.databaseDao = null;
        this.typeInfoCache = null;
        this.databaseComparator = null;
    }

    public PgDatabaseConverter(JdbcTemplate2 jdbcTemplate, boolean isMaster, String masterDatabaseVendor, DatabaseConfig databaseConfig) {
        super(jdbcTemplate, isMaster, masterDatabaseVendor, databaseConfig);
        this.databaseDao = new PgDatabaseDao(jdbcTemplate);
        this.databaseComparator = new PgDatabaseComparator(this);
        this.typeInfoCache = new TypeInfoCache(null, Integer.MAX_VALUE);
        this.truncateMetadata();
    }

    @Override
    public Logger getLogger() {
        return log;
    }

    @Override
    public List<String> getUnsupportedFunctions() {
        return new ArrayList<>();
    }

    @Override
    public void buildDatabaseFunctionMap(Map<DatabaseFunctionEnum, String[]> map) {

        // 数学函数
        map.put(MathematicalFunctionEnum.abs, new String[]{MathematicalFunctionEnum.abs.toString()});
        map.put(MathematicalFunctionEnum.cbrt, new String[]{MathematicalFunctionEnum.cbrt.toString()});
        map.put(MathematicalFunctionEnum.ceil, new String[]{MathematicalFunctionEnum.ceil.toString()});
        map.put(MathematicalFunctionEnum.ceiling, new String[]{MathematicalFunctionEnum.ceiling.toString()});
        map.put(MathematicalFunctionEnum.degrees, new String[]{MathematicalFunctionEnum.degrees.toString()});
        map.put(MathematicalFunctionEnum.div, new String[]{MathematicalFunctionEnum.div.toString()});
        map.put(MathematicalFunctionEnum.exp, new String[]{MathematicalFunctionEnum.exp.toString()});
        map.put(MathematicalFunctionEnum.factorial, new String[]{MathematicalFunctionEnum.factorial.toString()});
        map.put(MathematicalFunctionEnum.floor, new String[]{MathematicalFunctionEnum.floor.toString()});
        map.put(MathematicalFunctionEnum.gcd, new String[]{MathematicalFunctionEnum.gcd.toString()});
        map.put(MathematicalFunctionEnum.lcm, new String[]{MathematicalFunctionEnum.lcm.toString()});
        map.put(MathematicalFunctionEnum.ln, new String[]{MathematicalFunctionEnum.ln.toString()});
        map.put(MathematicalFunctionEnum.log, new String[]{MathematicalFunctionEnum.log.toString()});
        map.put(MathematicalFunctionEnum.log10, new String[]{MathematicalFunctionEnum.log10.toString()});
        map.put(MathematicalFunctionEnum.min_scale, new String[]{MathematicalFunctionEnum.min_scale.toString()});
        map.put(MathematicalFunctionEnum.mod, new String[]{MathematicalFunctionEnum.mod.toString()});
        map.put(MathematicalFunctionEnum.pi, new String[]{MathematicalFunctionEnum.pi.toString()});
        map.put(MathematicalFunctionEnum.power, new String[]{MathematicalFunctionEnum.power.toString()});
        map.put(MathematicalFunctionEnum.radians, new String[]{MathematicalFunctionEnum.radians.toString()});
        map.put(MathematicalFunctionEnum.round, new String[]{MathematicalFunctionEnum.round.toString()});
        map.put(MathematicalFunctionEnum.scale, new String[]{MathematicalFunctionEnum.scale.toString()});
        map.put(MathematicalFunctionEnum.sign, new String[]{MathematicalFunctionEnum.sign.toString()});
        map.put(MathematicalFunctionEnum.sqrt, new String[]{MathematicalFunctionEnum.sqrt.toString()});
        map.put(MathematicalFunctionEnum.trim_scale, new String[]{MathematicalFunctionEnum.trim_scale.toString()});
        map.put(MathematicalFunctionEnum.trunc, new String[]{MathematicalFunctionEnum.trunc.toString()});
        map.put(MathematicalFunctionEnum.width_bucket, new String[]{MathematicalFunctionEnum.width_bucket.toString()});
        map.put(MathematicalFunctionEnum.random, new String[]{MathematicalFunctionEnum.random.toString()});
        map.put(MathematicalFunctionEnum.acos, new String[]{MathematicalFunctionEnum.acos.toString()});
        map.put(MathematicalFunctionEnum.acosd, new String[]{MathematicalFunctionEnum.acosd.toString()});
        map.put(MathematicalFunctionEnum.asin, new String[]{MathematicalFunctionEnum.asin.toString()});
        map.put(MathematicalFunctionEnum.asind, new String[]{MathematicalFunctionEnum.asind.toString()});
        map.put(MathematicalFunctionEnum.atan, new String[]{MathematicalFunctionEnum.atan.toString()});
        map.put(MathematicalFunctionEnum.atand, new String[]{MathematicalFunctionEnum.atand.toString()});
        map.put(MathematicalFunctionEnum.atan2, new String[]{MathematicalFunctionEnum.atan2.toString()});
        map.put(MathematicalFunctionEnum.atan2d, new String[]{MathematicalFunctionEnum.atan2d.toString()});
        map.put(MathematicalFunctionEnum.cos, new String[]{MathematicalFunctionEnum.cos.toString()});
        map.put(MathematicalFunctionEnum.cosd, new String[]{MathematicalFunctionEnum.cosd.toString()});
        map.put(MathematicalFunctionEnum.cot, new String[]{MathematicalFunctionEnum.cot.toString()});
        map.put(MathematicalFunctionEnum.cotd, new String[]{MathematicalFunctionEnum.cotd.toString()});
        map.put(MathematicalFunctionEnum.sin, new String[]{MathematicalFunctionEnum.sin.toString()});
        map.put(MathematicalFunctionEnum.sind, new String[]{MathematicalFunctionEnum.sind.toString()});
        map.put(MathematicalFunctionEnum.tan, new String[]{MathematicalFunctionEnum.tan.toString()});
        map.put(MathematicalFunctionEnum.tand, new String[]{MathematicalFunctionEnum.tand.toString()});


        // 时间函数
        map.put(DateTypeFunctionEnum.to_char, new String[]{DateTypeFunctionEnum.to_char.toString()});
        map.put(DateTypeFunctionEnum.to_date, new String[]{DateTypeFunctionEnum.to_date.toString()});
        map.put(DateTypeFunctionEnum.to_number, new String[]{DateTypeFunctionEnum.to_number.toString()});
        map.put(DateTypeFunctionEnum.to_timestamp, new String[]{DateTypeFunctionEnum.to_timestamp.toString()});

        map.put(DateTypeFunctionEnum.age, new String[]{DateTypeFunctionEnum.age.toString()});
        map.put(DateTypeFunctionEnum.clock_timestamp, new String[]{DateTypeFunctionEnum.clock_timestamp.toString()});
        map.put(DateTypeFunctionEnum.current_date, new String[]{DateTypeFunctionEnum.current_date.toString()});
        map.put(DateTypeFunctionEnum.current_time, new String[]{DateTypeFunctionEnum.current_time.toString()});
        map.put(DateTypeFunctionEnum.current_timestamp, new String[]{DateTypeFunctionEnum.current_timestamp.toString()});
        map.put(DateTypeFunctionEnum.date_part, new String[]{DateTypeFunctionEnum.date_part.toString()});
        map.put(DateTypeFunctionEnum.date_trunc, new String[]{DateTypeFunctionEnum.date_trunc.toString()});
        map.put(DateTypeFunctionEnum.extract, new String[]{DateTypeFunctionEnum.extract.toString()});
        map.put(DateTypeFunctionEnum.isfinite, new String[]{DateTypeFunctionEnum.isfinite.toString()});
        map.put(DateTypeFunctionEnum.justify_days, new String[]{DateTypeFunctionEnum.justify_days.toString()});
        map.put(DateTypeFunctionEnum.justify_hours, new String[]{DateTypeFunctionEnum.justify_hours.toString()});
        map.put(DateTypeFunctionEnum.justify_interval, new String[]{DateTypeFunctionEnum.justify_interval.toString()});
        map.put(DateTypeFunctionEnum.localtime, new String[]{DateTypeFunctionEnum.localtime.toString()});
        map.put(DateTypeFunctionEnum.localtimestamp, new String[]{DateTypeFunctionEnum.localtimestamp.toString()});
        map.put(DateTypeFunctionEnum.make_date, new String[]{DateTypeFunctionEnum.make_date.toString()});
        map.put(DateTypeFunctionEnum.make_interval, new String[]{DateTypeFunctionEnum.make_interval.toString()});
        map.put(DateTypeFunctionEnum.make_time, new String[]{DateTypeFunctionEnum.make_time.toString()});
        map.put(DateTypeFunctionEnum.make_timestamp, new String[]{DateTypeFunctionEnum.make_timestamp.toString()});
        map.put(DateTypeFunctionEnum.make_timestamptz, new String[]{DateTypeFunctionEnum.make_timestamptz.toString()});
        map.put(DateTypeFunctionEnum.now, new String[]{DateTypeFunctionEnum.now.toString()});
        map.put(DateTypeFunctionEnum.statement_timestamp, new String[]{DateTypeFunctionEnum.statement_timestamp.toString()});
        map.put(DateTypeFunctionEnum.timeofday, new String[]{DateTypeFunctionEnum.timeofday.toString()});
        map.put(DateTypeFunctionEnum.transaction_timestamp, new String[]{DateTypeFunctionEnum.transaction_timestamp.toString()});


        // 字符函数
        map.put(StringFunctionEnum.bit_length, new String[]{StringFunctionEnum.bit_length.toString()});
        map.put(StringFunctionEnum.char_length, new String[]{StringFunctionEnum.char_length.toString()});
        map.put(StringFunctionEnum.character_length, new String[]{StringFunctionEnum.character_length.toString()});
        map.put(StringFunctionEnum.lower, new String[]{StringFunctionEnum.lower.toString()});
        map.put(StringFunctionEnum.normalize, new String[]{StringFunctionEnum.normalize.toString()});
        map.put(StringFunctionEnum.octet_length, new String[]{StringFunctionEnum.octet_length.toString()});
        map.put(StringFunctionEnum.overlay, new String[]{StringFunctionEnum.overlay.toString()});
        map.put(StringFunctionEnum.position, new String[]{StringFunctionEnum.position.toString()});
        map.put(StringFunctionEnum.substring, new String[]{StringFunctionEnum.substring.toString()});
        map.put(StringFunctionEnum.trim, new String[]{StringFunctionEnum.trim.toString()});
        map.put(StringFunctionEnum.upper, new String[]{StringFunctionEnum.upper.toString()});
        map.put(StringFunctionEnum.ascii, new String[]{StringFunctionEnum.ascii.toString()});
        map.put(StringFunctionEnum.btrim, new String[]{StringFunctionEnum.btrim.toString()});
        map.put(StringFunctionEnum.chr, new String[]{StringFunctionEnum.chr.toString()});
        map.put(StringFunctionEnum.concat, new String[]{StringFunctionEnum.concat.toString()});
        map.put(StringFunctionEnum.concat_ws, new String[]{StringFunctionEnum.concat_ws.toString()});
        map.put(StringFunctionEnum.format, new String[]{StringFunctionEnum.format.toString()});
        map.put(StringFunctionEnum.initcap, new String[]{StringFunctionEnum.initcap.toString()});
        map.put(StringFunctionEnum.left, new String[]{StringFunctionEnum.left.toString()});
        map.put(StringFunctionEnum.length, new String[]{StringFunctionEnum.length.toString()});
        map.put(StringFunctionEnum.lpad, new String[]{StringFunctionEnum.lpad.toString()});
        map.put(StringFunctionEnum.ltrim, new String[]{StringFunctionEnum.ltrim.toString()});
        map.put(StringFunctionEnum.md5, new String[]{StringFunctionEnum.md5.toString()});
        map.put(StringFunctionEnum.parse_ident, new String[]{StringFunctionEnum.parse_ident.toString()});
        map.put(StringFunctionEnum.quote_ident, new String[]{StringFunctionEnum.quote_ident.toString()});
        map.put(StringFunctionEnum.quote_literal, new String[]{StringFunctionEnum.quote_literal.toString()});
        map.put(StringFunctionEnum.quote_nullable, new String[]{StringFunctionEnum.quote_nullable.toString()});
        map.put(StringFunctionEnum.regexp_match, new String[]{StringFunctionEnum.regexp_match.toString()});
        map.put(StringFunctionEnum.regexp_matches, new String[]{StringFunctionEnum.regexp_matches.toString()});
        map.put(StringFunctionEnum.regexp_replace, new String[]{StringFunctionEnum.regexp_replace.toString()});
        map.put(StringFunctionEnum.regexp_split_to_array, new String[]{StringFunctionEnum.regexp_split_to_array.toString()});
        map.put(StringFunctionEnum.regexp_split_to_table, new String[]{StringFunctionEnum.regexp_split_to_table.toString()});
        map.put(StringFunctionEnum.repeat, new String[]{StringFunctionEnum.repeat.toString()});
        map.put(StringFunctionEnum.replace, new String[]{StringFunctionEnum.replace.toString()});
        map.put(StringFunctionEnum.reverse, new String[]{StringFunctionEnum.reverse.toString()});
        map.put(StringFunctionEnum.right, new String[]{StringFunctionEnum.right.toString()});
        map.put(StringFunctionEnum.rpad, new String[]{StringFunctionEnum.rpad.toString()});
        map.put(StringFunctionEnum.rtrim, new String[]{StringFunctionEnum.rtrim.toString()});
        map.put(StringFunctionEnum.split_part, new String[]{StringFunctionEnum.split_part.toString()});
        map.put(StringFunctionEnum.strpos, new String[]{StringFunctionEnum.strpos.toString()});
        map.put(StringFunctionEnum.substr, new String[]{StringFunctionEnum.substr.toString()});
        map.put(StringFunctionEnum.starts_with, new String[]{StringFunctionEnum.starts_with.toString()});
        map.put(StringFunctionEnum.to_ascii, new String[]{StringFunctionEnum.to_ascii.toString()});
        map.put(StringFunctionEnum.to_hex, new String[]{StringFunctionEnum.to_hex.toString()});
        map.put(StringFunctionEnum.translate, new String[]{StringFunctionEnum.translate.toString()});
        map.put(StringFunctionEnum.convert, new String[]{StringFunctionEnum.convert.toString()});
        map.put(StringFunctionEnum.convert_from, new String[]{StringFunctionEnum.convert_from.toString()});
        map.put(StringFunctionEnum.convert_to, new String[]{StringFunctionEnum.convert_to.toString()});
        map.put(StringFunctionEnum.encode, new String[]{StringFunctionEnum.encode.toString()});
        map.put(StringFunctionEnum.decode, new String[]{StringFunctionEnum.decode.toString()});

        // ERROR:  function gen_random_uuid() does not exist
        // LINE 1: select gen_random_uuid();
        //                ^
        // HINT:  No function matches the given name and argument types. You might need to add explicit type casts.
        // CREATE EXTENSION pgcrypto;
//     postgres=# select gen_random_uuid();
//     gen_random_uuid
// --------------------------------------
//     b9babd56-decd-4503-85ed-5250da5c2530
        map.put(StringFunctionEnum.uuid, new String[]{"gen_random_uuid()"});
    }

    @Override
    public BasicDatabaseDao getDatabaseDao() {
        return this.databaseDao;
    }

    @Override
    public String getPaginationSQL(TableInfo ti, int offset, int limit) {

        String tableName = this.getRightName(ti.getTableName());

        // 唯一键
        String colNames = this.getTableUniqueKeys(ti.getUniqueKeys());
        if (colNames.length() == 0) {
            // 无唯一键
            // 使用全部列
            colNames = this.getTableOrderColumnNames(ti.getColumns());
        }

        // 获取主键
        // 只有一个主键或一个唯一键
        if (ti.getUniqueKeys().size() == 1) {
            return String.format("SELECT "+this.getTableColumnNames(ti.getColumns(), "t1")+
                            " FROM %s t1 INNER JOIN (SELECT %s FROM %s ORDER BY %s LIMIT %s OFFSET %s) t2 ON t1.%s = t2.%s",
                    tableName, colNames, tableName, colNames, limit, offset, colNames, colNames);
        }
        // 普通分页查询
        return String.format("SELECT * FROM %s ORDER BY %s LIMIT %s OFFSET %s", tableName, colNames, limit, offset);
    }

    /**
     * 获取数据类型
     * {@link org.postgresql.jdbc.TypeInfoCache}
     */
    @Override
    protected void handleDataType(StringBuilder sqlBuilder, ColumnInfo columnInfo) {

        String typeName = "";   // 类型名称
        int length = -1;        // 字符长度
        int precision = -1;     // 精度
        int scale = -1;         // 刻度

        switch (columnInfo.getDataType()) {
            case Types.BIT:
            case Types.BOOLEAN:
                typeName = "bool";
                break;
            case Types.TINYINT:
            case Types.SMALLINT:
                typeName = "smallint";  // alias: int2
                break;
            case Types.INTEGER:
                typeName = "integer";   // alias: int4
                break;
            case Types.BIGINT:
                typeName = "bigint";    // alias: int8
                break;
            case Types.FLOAT:
                // 精度
                precision = Math.min(columnInfo.getColumnSize(), 53);
                typeName = "float";
                break;
            case Types.REAL:
                typeName = "real";    // alias: float4
                break;
            case Types.DOUBLE:
                typeName = "float8";
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                typeName = "numeric";

                // 精度，最大1000
                precision = Math.min(columnInfo.getColumnSize(), this.typeInfoCache.getMaximumPrecision(Oid.NUMERIC));
                // 刻度，最大1000
                if (columnInfo.getDecimalDigits() > 0) {
                    scale = Math.min(columnInfo.getDecimalDigits(), this.typeInfoCache.getMaximumPrecision(Oid.NUMERIC));
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
                // 默认为1
                if (columnInfo.getColumnSize() > this.typeInfoCache.getMaximumPrecision(Oid.BPCHAR)) {
                    // 10485760
                    typeName = "text";
                    break;
                }
                // 10485760
                length = Math.max(Math.min(columnInfo.getColumnSize(), this.typeInfoCache.getMaximumPrecision(Oid.BPCHAR)), 1);
                typeName = "char";
                break;
            case Types.NVARCHAR:
            case Types.VARCHAR:
                if (columnInfo.getColumnSize() > this.typeInfoCache.getMaximumPrecision(Oid.VARCHAR)) {
                    // 10485760
                    typeName = "text";
                    break;
                }
                length = Math.min(columnInfo.getColumnSize(), this.typeInfoCache.getMaximumPrecision(Oid.VARCHAR));
                typeName = "varchar";
                break;
            case Types.LONGNVARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
                typeName = "text";
                break;
            case Types.DATE:
                typeName = "date";
                break;
            case Types.TIME:
                typeName = "time";
                break;
            case Types.TIMESTAMP:
            case microsoft.sql.Types.DATETIME:
            case microsoft.sql.Types.SMALLDATETIME:
            case microsoft.sql.Types.DATETIMEOFFSET:
                // ERROR: type "datetime" does not exist
                typeName = "timestamp";
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                typeName = "bytea";
                break;

            case Types.ROWID:
                break;
            case Types.SQLXML:
                typeName = "xml";
                break;
            case Types.REF_CURSOR:
                typeName = "refcursor";
                break;
            case Types.TIME_WITH_TIMEZONE:
                typeName = "timetz";
                break;
            case Types.TIMESTAMP_WITH_TIMEZONE:
                typeName = "timestamptz";
                break;

            case microsoft.sql.Types.GEOMETRY:
                typeName = "geometry";
                break;
            case microsoft.sql.Types.GEOGRAPHY:
                typeName = "geography";
                break;
            case microsoft.sql.Types.SQL_VARIANT:
                typeName = "sql_variant";
                break;
            case microsoft.sql.Types.GUID:
                typeName = "CHAR";
                length = 36;
                break;
            case microsoft.sql.Types.MONEY:
                typeName = "money";
                break;
            case microsoft.sql.Types.SMALLMONEY:
                typeName = "numeric";
                scale = 4;
                precision = 10;
                break;

            case Types.NULL:
            case Types.OTHER:
            case Types.JAVA_OBJECT:
            case Types.DISTINCT:
            case Types.STRUCT:
            case Types.ARRAY:
            case Types.REF:
            case Types.DATALINK:
            default:
                typeName = columnInfo.getTypeName();

        }

        sqlBuilder.append(typeName);
        if (length > 0) {
            // 字符长度
            sqlBuilder.append("(").append(length).append(")");
        } else if (precision > 0) {
            // 数字精度、刻度
            sqlBuilder.append("(").append(precision);
            if (scale > 0) {
                sqlBuilder.append(", ").append(scale);
            }
            sqlBuilder.append(")");
        }
    }

    @Override
    public String getDatabaseVendor() {
        return DatabaseVendorEnum.POSTGRESQL.getVendor();
    }

    @Override
    public String getRightName(String name) {
        // postgres=# \d
        //          List of relations
        //  Schema |  Name  | Type  |  Owner
        // --------+--------+-------+----------
        //  public | TABLE2 | table | postgres
        //  public | table2 | table | postgres
        // 经测试发现，PostgreSQL使用双引号创建表的时候，会保留大小写。
        // 不用双引号的时候，默认会转成小写。
        //
        return "\"" + name.toLowerCase() + "\"";
    }

    @Override
    public String getDatabaseDataTypeDefault(String masterDataTypeDef, int dataType) {
        String defaultValue = super.getDatabaseDataTypeDefault(masterDataTypeDef, dataType);
        if (dataType == Types.BIT) {
            // bit -> boolean
            if ("1".equals(masterDataTypeDef)) {
                return "true";
            } else if ("0".equals(masterDataTypeDef)) {
                return "false";
            }
        }
        if(this.getDatabaseFunctionMap().get(StringFunctionEnum.uuid)[0].equals(defaultValue) && this.uuidStatus == null){
            // gen_random_uuid()
            // 检查是否启用了gen_random_uuid()。
            try{
                Map<String, Object> uuidRow = this.getDatabaseDao().getJdbcTemplate().queryForMap("select " + defaultValue);
                if(uuidRow.size() > 0){
                    this.uuidStatus = UUIDStatus.ENABLED;
                }
            }catch (Exception e){
                // ERROR: function gen_random_uuid() does not exist
                this.getLogger().error(e.getMessage());
                this.getLogger().info("[{}] CREATE EXTENSION pgcrypto", this.getDatabaseConfig().getName());
                this.uuidStatus = UUIDStatus.NOT_EXISTS;
                try {
                    this.getDatabaseDao().getJdbcTemplate().execute("CREATE EXTENSION pgcrypto");

                    Map<String, Object> uuidRow = this.getDatabaseDao().getJdbcTemplate().queryForMap("select " + defaultValue);
                    if(uuidRow.size() > 0){
                        this.uuidStatus = UUIDStatus.ENABLED;
                    }

                } catch (Exception e2){
                    this.getLogger().error(e2.getMessage());
                    this.uuidStatus = UUIDStatus.NO_PERMISSION;
                }
            }

            if(!this.uuidStatus.equals(UUIDStatus.ENABLED)){
                this.getLogger().warn("[{}] ERROR:  function gen_random_uuid() does not exist！expect:[ CREATE EXTENSION pgcrypto ]", this.getDatabaseConfig().getName());
            }

        }
        return defaultValue;
    }

    @Override
    public DatabaseComparator getComparator() {
        return this.databaseComparator;
    }


}
