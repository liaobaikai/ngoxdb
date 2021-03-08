package com.liaobaikai.ngoxdb.core.service.impl.converter;

import com.liaobaikai.ngoxdb.bean.info.ColumnInfo;
import com.liaobaikai.ngoxdb.bean.info.IndexInfo2;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.bean.rs.ImportedKey;
import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
import com.liaobaikai.ngoxdb.core.config.DatabaseConfig;
import com.liaobaikai.ngoxdb.core.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.core.dao.impl.SQLServerDatabaseDao;
import com.liaobaikai.ngoxdb.core.entity.SlaveMetaDataEntity;
import com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum;
import com.liaobaikai.ngoxdb.core.enums.IndexTypeEnum;
import com.liaobaikai.ngoxdb.core.enums.fn.DatabaseFunctionEnum;
import com.liaobaikai.ngoxdb.core.enums.fn.DateTypeFunctionEnum;
import com.liaobaikai.ngoxdb.core.enums.fn.MathematicalFunctionEnum;
import com.liaobaikai.ngoxdb.core.enums.fn.StringFunctionEnum;
import com.liaobaikai.ngoxdb.core.service.DatabaseComparator;
import com.liaobaikai.ngoxdb.core.service.impl.comparator.SQLServerDatabaseComparator;
import com.liaobaikai.ngoxdb.utils.NumberUtils;
import com.liaobaikai.ngoxdb.utils.StringUtils;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.sun.org.apache.xpath.internal.operations.Bool;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Microsoft SQLServer 转换器
 *
 * @author baikai.liao
 * @Time 2021-01-28 15:56:41
 */
@Slf4j
@Service
public class SQLServerDatabaseConverter extends BasicDatabaseConverter {

    private Map<String, Boolean> tableForeignKeyRuleMap = new HashMap<>();

    public static final BigDecimal MAX_VALUE_MONEY = new BigDecimal("922337203685477.5807");
    public static final BigDecimal MIN_VALUE_MONEY = new BigDecimal("-922337203685477.5808");
    public static final BigDecimal MAX_VALUE_SMALLMONEY = new BigDecimal("214748.3647");
    public static final BigDecimal MIN_VALUE_SMALLMONEY = new BigDecimal("-214748.3648");
    public final static int SHORT_VARTYPE_MAX_CHARS = 4000;
    public final static int SHORT_VARTYPE_MAX_BYTES = 8000;
    public final static int SQL_USHORTVARMAXLEN = 65535; // 0xFFFF
    public final static int NTEXT_MAX_CHARS = 0x3FFFFFFF;
    public final static int IMAGE_TEXT_MAX_BYTES = 0x7FFFFFFF;
    public final static int MAX_VARTYPE_MAX_CHARS = 0x3FFFFFFF;
    public final static int MAX_VARTYPE_MAX_BYTES = 0x7FFFFFFF;
    public static final int MAXTYPE_LENGTH = 0xFFFF;
    public static final int UNKNOWN_STREAM_LENGTH = -1;

    /**
     * 只适用于SQLServer，将timestamp类型转换为binary，不然无法导入数据，会抛出如下错误：
     * 不能将显式值插入时间戳列。请对列列表使用 INSERT 来排除时间戳列，或将 DEFAULT 插入时间戳列。
     */
    private boolean timestamp2binary = true;

    private final SQLServerDatabaseDao databaseDao;
    private final SQLServerDatabaseComparator databaseComparator;

    public SQLServerDatabaseConverter() {
        this.databaseDao = null;
        this.databaseComparator = null;
    }

    public SQLServerDatabaseConverter(JdbcTemplate2 jdbcTemplate,
                                      boolean isMaster,
                                      String masterDatabaseVendor,
                                      DatabaseConfig databaseConfig) {
        super(jdbcTemplate, isMaster, masterDatabaseVendor, databaseConfig);
        this.databaseDao = new SQLServerDatabaseDao(jdbcTemplate);
        this.databaseComparator = new SQLServerDatabaseComparator(this);
        this.truncateMetadata();
    }

    @Override
    public Logger getLogger() {
        return log;
    }

    @Override
    public List<ColumnInfo> getColumns(String... tableName) {
        List<ColumnInfo> columns = super.getColumns(tableName);

        columns.forEach(columnInfo -> {

            // 生成columnType.
            StringBuilder columnType = new StringBuilder();
            this.handleDataType(columnType, columnInfo);
            columnInfo.setColumnType(columnType.toString());

            // 替换掉最外层双括号，如：((1)) ((getdate()))
            if (columnInfo.getColumnDef() != null) {
                String s = columnInfo.getColumnDef()
                        .replaceFirst("^\\(", "")
                        .replaceFirst("\\)$", "");
                if (s.charAt(0) == '(') {
                    s = s.replaceFirst("^\\(", "").replaceFirst("\\)$", "");
                }
                columnInfo.setColumnDef(s);
            }

        });
        return columns;
    }

    @Override
    public String getDatabaseDataTypeDefault(String masterDataTypeDef, int dataType) {
        String defaultValue = super.getDatabaseDataTypeDefault(masterDataTypeDef, dataType);
        if (dataType == Types.BOOLEAN || dataType == Types.BIT || dataType == Types.NUMERIC) {
            if ("true".equals(masterDataTypeDef)) {
                return "1";
            } else if ("false".equals(masterDataTypeDef)) {
                return "0";
            }
        }
        return defaultValue;
    }

    @Override
    public List<String> getUnsupportedFunctions() {
        return new ArrayList<String>() {
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

        // 唯一键
        String colNames = this.getTableUniqueKeys(ti.getUniqueKeys());
        if (colNames.length() == 0) {
            // 无唯一键
            // 使用全部列
            colNames = this.getTableOrderColumnNames(ti.getColumns());
        }

        if (offset == 0) {
            // 第一页使用top
            return "SELECT TOP " + limit + " * FROM " + this.getRightName(ti.getTableName()) + " ORDER BY " + colNames;
        }

        return "SELECT TOP " + limit + " " + this.getTableColumnNames(ti.getColumns(), "T") + " FROM (" +
                "  SELECT *, ROW_NUMBER() OVER( ORDER BY " + colNames + " ) AS _ROW_NUMBER FROM " + this.getRightName(ti.getTableName()) +
                ") T WHERE T._ROW_NUMBER > " + offset;
    }

    @Override
    public void buildIndex(TableInfo ti) {
        // 构建索引
        // https://docs.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql?view=sql-server-ver15
        if (ti == null || ti.getIndexInfo() == null) {
            return;
        }

        // 一个键名，多个列
        StringBuilder sBuilder = null;
        List<String> buildIndex = null;
        String indexType;

        for (IndexInfo2 ii : ti.getIndexInfo()) {

            if (ii.isTableIndexStatistic()) {
                // 统计信息不需要。
                continue;
            }

            if (sBuilder == null) {
                sBuilder = new StringBuilder();
            }
            if (buildIndex == null) {
                buildIndex = new ArrayList<>();
            }

            if (buildIndex.contains(ii.getIndexName())) {
                continue;
            }

            buildIndex.add(ii.getIndexName());

            sBuilder.append("create ");

            if (!ii.isNonUnique()) {
                indexType = SlaveMetaDataEntity.TYPE_UNIQUE_INDEX;
                sBuilder.append("unique ");

            } else if (IndexTypeEnum.FULLTEXT.toString().equalsIgnoreCase(ii.getIndexTypeDesc())) {
                // 全文索引
                indexType = SlaveMetaDataEntity.TYPE_FULLTEXT_INDEX;
                sBuilder.append("fulltext index ");

            /*} else if(IndexTypeEnum.SPATIAL.toString().equalsIgnoreCase(ii.getIndexTypeDesc())){
                // 空间索引
                sBuilder.append("ADD ").append(ii.getIndexTypeDesc()).append(" INDEX ");
                indexType = SlaveMetaDataEntity.TYPE_SPATIAL_INDEX;
            */
            } else {
                indexType = SlaveMetaDataEntity.TYPE_INDEX;
            }

            if(!indexType.equals(SlaveMetaDataEntity.TYPE_FULLTEXT_INDEX)){
                switch (ii.getType()) {
                    case DatabaseMetaData.tableIndexClustered:
                        sBuilder.append("clustered index ");
                        break;
                    case DatabaseMetaData.tableIndexHashed:
                    case DatabaseMetaData.tableIndexOther:
                        sBuilder.append("nonclustered index ");
                        break;
                }

                // [index_name]
                String indexName = ii.getIndexName();
                if (this.getDatabaseConfig().isGenerateName()) {
                    // 自动生成
                    indexName = this.buildIndexName(ii, ti);
                }
                sBuilder.append(this.getRightName(indexName));
            }

            sBuilder.append(" on ").append(this.getRightName(ii.getTableName()));

            // column-list
            sBuilder.append("(");

            // 获取所有的columnName
            for (IndexInfo2 ii2 : ti.getIndexInfo()) {
                if (!ii.getIndexName().equals(ii2.getIndexName())) {
                    continue;
                }
                sBuilder.append(this.getRightName(ii2.getColumnName())).append(" ").append(ii2.getOrder()).append(",");
            }

            sBuilder.delete(sBuilder.length() - 1, sBuilder.length()).append(")");

            if (IndexTypeEnum.FULLTEXT.toString().equalsIgnoreCase(ii.getIndexTypeDesc())
                    && ti.getPrimaryKeys().size() == 1) {
                // 全文索引
                // https://docs.microsoft.com/zh-cn/sql/t-sql/statements/create-fulltext-index-transact-sql?view=sql-server-ver15
                // https://zhuanlan.zhihu.com/p/159211413
                // 查询主键的名称
                List<Map<String, Object>> pkList =
                        this.getJdbcTemplate().queryForList(
                                "SELECT NAME FROM SYS.OBJECTS WHERE PARENT_OBJECT_ID = OBJECT_ID(?) AND TYPE = 'PK'", ti.getTableName());

                if(pkList.size() == 1){
                    String fullTextCatalogName = ti.getTableName() + "_catalog";

                    Map<String, Object> result =
                            this.getJdbcTemplate().queryForMap("SELECT COUNT(NAME) AS COUNT FROM SYS.FULLTEXT_CATALOGS WHERE NAME = ?", fullTextCatalogName);
                    int exists = NumberUtils.toInt(result.get("COUNT"));
                    if(exists == 0){
                        this.databaseDao.insertMetadata(new SlaveMetaDataEntity(ti.getTableName(), SlaveMetaDataEntity.TYPE_OTHER,
                                "create fulltext catalog " + fullTextCatalogName));
                    }

                    sBuilder.append(" key index ").append(pkList.get(0).get("NAME"))
                            .append(" on ").append(fullTextCatalogName)
                            .append(" with change_tracking auto");
                }
            }

            this.databaseDao.insertMetadata(new SlaveMetaDataEntity(ti.getTableName(), indexType, sBuilder.toString()));

            // 全部清掉
            sBuilder.delete(0, sBuilder.length());
        }
    }

    @Override
    public void buildComment(TableInfo ti) {
        // 构建注释
        // https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-addextendedproperty-transact-sql?view=sql-server-ver15

        final String tName = ti.getTableName();

        // 列注释
        ti.getColumns().forEach(c -> {
            if (StringUtils.isEmpty(c.getRemarks())) {
                return;
            }

            String s = String.format("EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'%s' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'%s', @level2type=N'COLUMN',@level2name=N'%s'",
                    c.getRemarks(), tName, c.getColumnName());
            this.getDatabaseDao().insertMetadata(new SlaveMetaDataEntity(tName, SlaveMetaDataEntity.TYPE_COMMENT, s));

        });

        // 表注释
        if (StringUtils.isNotEmpty(ti.getRemarks())) {

            String s = String.format("EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'%s' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'%s'",
                    ti.getRemarks(), tName);
            this.getDatabaseDao().insertMetadata(new SlaveMetaDataEntity(tName, SlaveMetaDataEntity.TYPE_COMMENT, s));
        }
    }

    @Override
    protected void handleDataType(StringBuilder sqlBuilder, ColumnInfo columnInfo) {
        // timestamp -> binary
        // https://stackoverflow.com/questions/31377666/why-sql-server-timestamp-type-is-mapped-to-binary-type-in-hibernate

        String typeName = "";   // 类型名称
        int length = -1;        // 字符长度
        int precision = -1;     // 精度
        int scale = -1;         // 刻度

        switch (columnInfo.getDataType()) {
            case Types.BOOLEAN:
                // https://stackoverflow.com/questions/1777257/how-do-you-create-a-yes-no-boolean-field-in-sql-server
            case Types.BIT:
                // 第1 个列、参数或变量: 不能对数据类型bit 指定列宽。
                typeName = "bit";
                break;
            case Types.TINYINT:
                typeName = "tinyint";
                break;
            case Types.SMALLINT:
                typeName = "smallint";
                break;
            case Types.INTEGER:
                typeName = "int";
                break;
            case Types.BIGINT:
                typeName = "bigint";
                break;
            case Types.FLOAT:
            case Types.DOUBLE:
                typeName = "float";
                break;
            case Types.REAL:
                typeName = "real";
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                // 精度、刻度
                // 指定的列精度 65 大于最大精度 38。
                typeName = "numeric";
                precision = Math.min(columnInfo.getColumnSize(), 38);
                if (columnInfo.getDecimalDigits() > 0) {
                    scale = Math.min(columnInfo.getDecimalDigits(), 38);
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
            case Types.CHAR:
            case Types.VARCHAR:
                if ("uniqueidentifier".equals(columnInfo.getTypeName())) {
                    typeName = "uniqueidentifier";
                    break;
                }
                if (columnInfo.getColumnSize() > SHORT_VARTYPE_MAX_CHARS * 2) {
                    // 最多8000个英文，4000个汉字
                    typeName = "varchar(max)";
                    break;
                }
                typeName = columnInfo.getDataType() == Types.CHAR ? "char" : "varchar";
                length = columnInfo.getColumnSize();
                break;
            case Types.CLOB:
            case Types.LONGVARCHAR:
                // https://stackoverflow.com/questions/834788/using-varcharmax-vs-text-on-sql-server
                typeName = "varchar(max)";
                break;
            case Types.DATE:
                typeName = "date";
                break;
            case Types.TIME_WITH_TIMEZONE:
            case Types.TIME:
                // time(7)
                if (columnInfo.getDecimalDigits() >= 0) {
                    precision = columnInfo.getDecimalDigits();
                }
                typeName = "time";
                break;
            case Types.TIMESTAMP_WITH_TIMEZONE:
            case Types.TIMESTAMP:
                if (columnInfo.getDecimalDigits() >= 0) {
                    precision = columnInfo.getDecimalDigits();
                }
                typeName = "datetime2";
                break;
            case Types.BINARY:
                // sqlserver: timestamp
                if ("timestamp".equals(columnInfo.getTypeName())) {
                    if (!this.timestamp2binary) {
                        // 不可为空的 timestamp 列在语义上等同于 binary(8) 列。
                        typeName = "timestamp";
                        break;
                    } else {
                        length = 8;
                    }
                } else if (columnInfo.getColumnSize() > SHORT_VARTYPE_MAX_CHARS * 2) {
                    typeName = "varbinary(max)";
                    break;
                } else {
                    length = columnInfo.getColumnSize();
                }
                typeName = "binary";
                break;
            case Types.VARBINARY:
                // sqlserver: udt
                if ("udt".equals(columnInfo.getTypeName())) {
                    typeName = "udt";
                    break;
                }

                if ("timestamp".equals(columnInfo.getTypeName())) {
                    if (!this.timestamp2binary) {
                        // 可为空的 timestamp 列在语义上等同于 varbinary(8) 列。
                        typeName = "timestamp";
                        break;
                    } else {
                        length = 8;
                    }
                }

                if (columnInfo.getColumnSize() > SHORT_VARTYPE_MAX_CHARS * 2) {
                    typeName = "varbinary(max)";
                    break;
                } else {
                    length = columnInfo.getColumnSize();
                }
                typeName = "varbinary";

                break;
            case Types.LONGVARBINARY:
                // sqlserver: image
                if ("image".equals(columnInfo.getTypeName())) {
                    typeName = "image";
                    break;
                }
                typeName = "varbinary(max)";
                break;
            // case Types.NULL:
            //     typeName = "null";
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
            case Types.BLOB:
                typeName = "image";
                break;
            // case Types.REF:
            //     break;
            // case Types.DATALINK:
            //     break;

            case Types.ROWID:
                break;
            case Types.NCHAR:
            case Types.NVARCHAR:
                if (columnInfo.getColumnSize() > SHORT_VARTYPE_MAX_CHARS) {
                    // 可存储4000个字符，无论英文还是汉字
                    typeName = "nvarchar(max)";
                    break;
                }
                typeName = columnInfo.getDataType() == Types.NCHAR ? "nchar" : "nvarchar";
                length = columnInfo.getColumnSize();
                break;
            case Types.LONGNVARCHAR:
            case Types.NCLOB:
                // sqlserver: xml
                if ("xml".equals(columnInfo.getTypeName())) {
                    typeName = "xml";
                    break;
                }
                typeName = "nvarchar(max)";
                break;
            case Types.SQLXML:
                break;
            // case Types.REF_CURSOR:
            //     break;

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
                typeName = "uniqueidentifier";
                break;
            case microsoft.sql.Types.DATETIME:
                typeName = "datetime";
                break;
            case microsoft.sql.Types.SMALLDATETIME:
                typeName = "smalldatetime";
                break;
            case microsoft.sql.Types.DATETIMEOFFSET:
                typeName = "datetimeoffset";
                precision = columnInfo.getDecimalDigits();
                break;
            case microsoft.sql.Types.MONEY:
                typeName = "money";
                break;
            case microsoft.sql.Types.SMALLMONEY:
                typeName = "smallmoney";
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
    protected void buildForeignKeyChangeRules(StringBuilder sBuilder, ImportedKey importedKey) {
        // 将 FOREIGN KEY 约束 'a' 引入表 'b' 可能会导致循环或多重级联路径。请指定 ON DELETE NO ACTION 或 ON UPDATE NO ACTION，或修改其他 FOREIGN KEY 约束。
        // https://docs.microsoft.com/zh-cn/sql/relational-databases/errors-events/mssqlserver-1785-database-engine-error?view=sql-server-ver15
        // sqlserver不支持存在多个cascade的外键。
        Boolean isDeletedCascade = this.tableForeignKeyRuleMap.get(importedKey.getPkTableName() + "-deleted");
        Boolean isUpdatedCascade = this.tableForeignKeyRuleMap.get(importedKey.getPkTableName() + "-updated");
        boolean f1 = false, f2 = false;

        // 已经存在了cascade
        if(isDeletedCascade != null && isDeletedCascade){
            // 关键字 'RESTRICT' 附近有语法错误。
            // https://docs.microsoft.com/en-us/sql/relational-databases/tables/primary-and-foreign-key-constraints?view=sql-server-ver15
            // Cascading Referential Integrity
            if(importedKey.getDeleteRule() == ImportedKey.CASCADE){
                this.getLogger().warn("{}.{} ON DELETE CASCADE -> ON DELETE NO ACTION", importedKey.getPkTableName(), importedKey.getPkColumnName());
                sBuilder.append(" ON DELETE ").append(importedKey.getActionName(ImportedKey.NO_ACTION)).append(" ");
                f1 = true;
            }
        }

        if (importedKey.getDeleteRule() != ImportedKey.RESTRICT && !f1) {
            sBuilder.append(" ").append(importedKey.getDeleteAction()).append(" ");
        }

        // 更新规则
        if(isUpdatedCascade != null && isUpdatedCascade){
            if(importedKey.getUpdateRule() == ImportedKey.CASCADE){
                this.getLogger().warn("{}.{} ON UPDATE CASCADE -> ON UPDATE NO ACTION", importedKey.getPkTableName(), importedKey.getPkColumnName());
                sBuilder.append(" ON UPDATE ").append(importedKey.getActionName(ImportedKey.NO_ACTION)).append(" ");
                f2 = true;
            }
        }

        if (importedKey.getUpdateRule() != ImportedKey.RESTRICT && !f2) {
            sBuilder.append(" ").append(importedKey.getUpdateAction()).append(" ");
        }

        // 一个主键表，存在多个外键的cascade引用的话，需要将其他改成NO ACTION
        this.tableForeignKeyRuleMap.put(importedKey.getPkTableName() + "-deleted", importedKey.getDeleteRule() == ImportedKey.CASCADE);
        this.tableForeignKeyRuleMap.put(importedKey.getPkTableName() + "-updated", importedKey.getUpdateRule() == ImportedKey.CASCADE);

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

    @Override
    public String getAutoincrement() {
        return "IDENTITY(1, 1)";
    }

    // @Override
    // public void changeBatchArgs(List<Object[]> batchArgs, TableInfo ti) {
    //     // https://docs.microsoft.com/zh-cn/previous-versions/sql/sql-server-2005/ms182776(v=sql.90)?redirectedfrom=MSDN
    //
    //     // TIMESTAMP 不支持设置值。
    //     // 否则会抛出如下错误：
    //     // 不能将显式值插入时间戳列。请对列列表使用 INSERT 来排除时间戳列，或将 DEFAULT 插入时间戳列。
    //
    //     // 需要将具体的参数值设置为 null，SQLServer会自动生成。
    //     for(Object[] objects : batchArgs){
    //         // row
    //         for(int x = 0, len = objects.length; x < len; x++){
    //             // cell
    //             ColumnInfo columnInfo = ti.getColumns().get(x);
    //             if(this.isSameDatabaseVendor() && "timestamp".equals(columnInfo.getTypeName())){
    //                 objects[x] = null;
    //             }
    //         }
    //     }
    // }

    @Override
    public DatabaseComparator getComparator() {
        return this.databaseComparator;
    }

}
