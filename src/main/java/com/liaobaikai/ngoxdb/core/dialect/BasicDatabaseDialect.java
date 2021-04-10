package com.liaobaikai.ngoxdb.core.dialect;

import com.liaobaikai.ngoxdb.bean.ColumnType;
import com.liaobaikai.ngoxdb.bean.PreparedPagination;
import com.liaobaikai.ngoxdb.bean.info.ColumnInfo;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.core.constant.JdbcDataType;
import com.liaobaikai.ngoxdb.core.enums.func.DatabaseFunctionEnum;
import com.liaobaikai.ngoxdb.core.func.SQLFunction;

import java.sql.SQLException;
import java.util.*;

/**
 * 数据库方言基类
 *
 * @author baikai.liao
 * @Time 2021-03-12 11:44:52
 */
public abstract class BasicDatabaseDialect implements DatabaseDialect {

    /**
     * SQL函数注册列表
     */
    private final Map<DatabaseFunctionEnum, SQLFunction[]> regSQLFunctionMap = new LinkedHashMap<>();

    /**
     * jdbc类型注册
     */
    private final Map<Integer, String> dataTypeMap = new HashMap<>(JdbcDataType.JDBC_DATA_TYPE_NAME_BINDER);

    @Override
    public Map<Integer, String> getDataTypeMap() {
        return this.dataTypeMap;
    }

    @Override
    public Map<DatabaseFunctionEnum, SQLFunction[]> getSQLFunctionMap() {
        return this.regSQLFunctionMap;
    }

    /**
     * 注册SQL函数
     *
     * @param fe        函数类型
     * @param functions 注册的函数列表
     */
    public void registerFunction(DatabaseFunctionEnum fe, SQLFunction... functions) {
        this.regSQLFunctionMap.put(fe, functions);
    }

    /**
     * 注册类型
     *
     * @param jdbcDataType {@link JdbcDataType}
     * @param typeName     注册的类型名称
     */
    public void registerColumnType(int jdbcDataType, String typeName) {
        this.dataTypeMap.put(jdbcDataType, typeName);
    }

    @Override
    public String toLookupName(String src) {
        if (defaultUpperCaseName()) {
            src = src.toUpperCase();
        } else if (defaultLowerCaseName()) {
            src = src.toLowerCase();
        }
        return openQuote() + src + closeQuote();
    }

    public String join(String[] names) {
        return join(names, null);
    }

    public String join(String[] names, String tableAlias) {
        StringBuilder sBuilder = new StringBuilder();
        for (int i = 0, len = names.length; i < len; i++) {
            if (tableAlias != null) {
                sBuilder.append(tableAlias).append(".");
            }
            sBuilder.append(this.toLookupName(names[i]));
            if (i != len - 1) {
                sBuilder.append(",");
            }
        }
        return sBuilder.toString();
    }

    @Override
    public String getAddForeignKeyConstraintString(final String constraintName,
                                                   final String[] foreignKey,
                                                   final String referencedTableName,
                                                   final String[] referencedPrimaryKeys) {

        return " add constraint " + this.toLookupName(constraintName) +
                " foreign key (" + this.join(foreignKey) + ") references " +
                this.toLookupName(referencedTableName) + " (" + this.join(referencedPrimaryKeys) + ')';
    }

    @Override
    public PreparedPagination getPreparedPagination(final String tableName,
                                                    final String[] queryColumnNames,
                                                    final String[] orderColumnNames,
                                                    final boolean isPrimaryKeyOrder,
                                                    final int offset,
                                                    final int limit) {

        PreparedPagination preparedPagination = new PreparedPagination();
        // 获取主键
        // 只有一个主键或一个唯一键
        if (isPrimaryKeyOrder && orderColumnNames.length == 1) {
            String pk = orderColumnNames[0];
            preparedPagination.setPreparedSql(String.format("select %s from %s t join (select %s from %s order by %s limit ? offset ?) t0 on t.%s = t0.%s",
                    this.join(queryColumnNames, "t"),
                    this.toLookupName(tableName),
                    pk,
                    this.toLookupName(tableName),
                    pk,
                    pk,
                    pk));
        } else {
            // 普通分页查询
            preparedPagination.setPreparedSql(String.format("select %s from %s t order by %s limit ? offset ?",
                    this.join(queryColumnNames, "t"),
                    this.toLookupName(tableName),
                    this.join(orderColumnNames)));
        }

        Object[] paramValues = new Object[2];
        paramValues[0] = limit;
        paramValues[1] = offset;
        preparedPagination.setParamValues(paramValues);

        return preparedPagination;
    }

    @Override
    public boolean supportsCommentOn() {
        return false;
    }

    @Override
    public boolean supportsCommentOnBuildTable() {
        return false;
    }

    @Override
    public String getTableCommentString(String tableName, String tableComment) {
        return null;
    }

    @Override
    public String getColumnCommentString(String tableName, String columnName, String columnComment) {
        return null;
    }

    @Override
    public boolean supportsCollationOnBuildTable() {
        return false;
    }

    @Override
    public String getCollationString(String collationName) {
        return null;
    }

    @Override
    public boolean supportsCharsetOnBuildTable() {
        return false;
    }

    @Override
    public String getCharsetString(String charsetName) {
        return null;
    }

    @Override
    public boolean supportsDropForeignKey() {
        return false;
    }

    @Override
    public String getDropForeignKeyString() {
        return " drop constraint ";
    }

    @Override
    public boolean supportsPartitionBy() {
        return false;
    }

    @Override
    public boolean supportsIfExistsBeforeTableName() {
        return false;
    }

    @Override
    public String getNullColumnString() {
        return null;
    }

    @Override
    public boolean supportsSequences() {
        return false;
    }

    @Override
    public String getCreateSequenceString(String sequenceName, long minvalue, long maxvalue, long startWith, long step) {
        return null;
    }

    @Override
    public String getDropSequenceString(String sequenceName) {
        return null;
    }

    @Override
    public boolean supportsIdentityReplaceColumnType() {
        return false;
    }

    @Override
    public boolean supportsIdentityColumns() {
        return false;
    }

    @Override
    public boolean doesIdentityMustPrimaryKey() {
        return false;
    }

    @Override
    public String getIdentityColumnString() {
        return null;
    }

    @Override
    public boolean supportsDisableIdentity() {
        return false;
    }

    @Override
    public String getDisableIdentityString(String tableName) {
        return null;
    }

    @Override
    public String getEnableIdentityString(String tableName) {
        return null;
    }

    @Override
    public boolean supportsCascadeDelete() {
        return false;
    }

    @Override
    public boolean supportsLimit() {
        return false;
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
        return false;
    }

    @Override
    public int getMaximumPrecision(int jdbcDataType) {
        return 0;
    }

    @Override
    public int getMinimumPrecision(int jdbcDataType) {
        return 0;
    }

    @Override
    public int getMaximumScale(int jdbcDataType) {
        return 0;
    }

    @Override
    public int getMinimumScale(int jdbcDataType) {
        return 0;
    }

    @Override
    public boolean supportsDecimalToInt() {
        return false;
    }

    @Override
    public boolean supportsYearToDate() {
        return false;
    }

    @Override
    public boolean supportsJoin() {
        return false;
    }

    @Override
    public Object getSqlGenericType(Object src) throws SQLException {
        return src;
    }

    @Override
    public boolean supportsBitmapIndex() {
        return false;
    }

    @Override
    public boolean supportsFullTextIndex() {
        return false;
    }

    @Override
    public boolean supportsSpatialIndex() {
        return false;
    }

    @Override
    public boolean supportsClusteredIndex() {
        return false;
    }

    @Override
    public boolean supportsGlobalPartitionIndex() {
        return false;
    }

    @Override
    public boolean supportsLocalPartitionIndex() {
        return false;
    }

    /**
     * 获取表的可以排序的列名，如clob类型的列不支持
     *
     * @param tableColumns 所有列
     * @return col1, col2, col3, ...
     */
    @Override
    public String[] getTableOrderKeys(List<ColumnInfo> tableColumns) {
        List<String> orderKeys = new ArrayList<>();
        for (ColumnInfo columnInfo : tableColumns) {
            switch (columnInfo.getDataType()) {
                case java.sql.Types.BLOB:
                case java.sql.Types.BINARY:
                case java.sql.Types.VARBINARY:
                case java.sql.Types.CLOB:
                case java.sql.Types.NCLOB:
                case java.sql.Types.LONGNVARCHAR:
                case java.sql.Types.LONGVARCHAR:
                case java.sql.Types.LONGVARBINARY:
                    continue;
                case java.sql.Types.CHAR:
                case java.sql.Types.NCHAR:
                case java.sql.Types.VARCHAR:
                case java.sql.Types.NVARCHAR:
                    if (columnInfo.getColumnSize() > 255) {
                        // access database varchar最大是255，因此为了兼容其他数据库，大于255字段的都需要排除。
                        continue;
                    }
                    break;
            }
            orderKeys.add(columnInfo.getColumnName());
        }
        return orderKeys.toArray(new String[0]);
    }

    @Override
    public boolean supportsUnsignedDecimal() {
        return false;
    }

    /**
     * 转换列类型
     *
     * @param ci 列信息
     * @return {@link ColumnType}
     */
    @Override
    public ColumnType getColumnType(ColumnInfo ci) {

        final ColumnType ct = new ColumnType(this.getDataTypeMap());
        ct.setColumnName(ci.getColumnName());
        ct.setUnsigned(ci.isUnsigned() & this.supportsUnsignedDecimal());
        ct.setJdbcDataType(ci.getDataType());

        switch (ct.getJdbcDataType()) {
            case JdbcDataType.BIT:
                this.registerColumnTypeForBit(ct, ci);
                break;
            case JdbcDataType.TINYINT:
                this.registerColumnTypeForTinyint(ct, ci);
                break;
            case JdbcDataType.SMALLINT:
                this.registerColumnTypeForSmallint(ct, ci);
                break;
            case JdbcDataType.INTEGER:
                this.registerColumnTypeForInt(ct, ci);
                break;
            case JdbcDataType.BIGINT:
                this.registerColumnTypeForBigInt(ct, ci);
                break;
            case JdbcDataType.FLOAT:
                this.registerColumnTypeForFloat(ct, ci);
                break;
            case JdbcDataType.REAL:
                this.registerColumnTypeForReal(ct, ci);
                break;
            case JdbcDataType.DOUBLE:
                this.registerColumnTypeForDouble(ct, ci);
                break;
            case JdbcDataType.NUMERIC:
                this.registerColumnTypeForNumeric(ct, ci);
                break;
            case JdbcDataType.DECIMAL:
                this.registerColumnTypeForDecimal(ct, ci);
                break;
            case JdbcDataType.CHAR:
                this.registerColumnTypeForChar(ct, ci);
                break;
            case JdbcDataType.VARCHAR:
                this.registerColumnTypeForVarchar(ct, ci);
                break;
            case JdbcDataType.LONGVARCHAR:
                this.registerColumnTypeForLongVarchar(ct, ci);
                break;
            case JdbcDataType.DATE:
                this.registerColumnTypeForDate(ct, ci);
                break;
            case JdbcDataType.TIME:
                this.registerColumnTypeForTime(ct, ci);
                break;
            case JdbcDataType.TIMESTAMP:
                this.registerColumnTypeForTimestamp(ct, ci);
                break;
            case JdbcDataType.BINARY:
                this.registerColumnTypeForBinary(ct, ci);
                break;
            case JdbcDataType.VARBINARY:
                this.registerColumnTypeForVarbinary(ct, ci);
                break;
            case JdbcDataType.LONGVARBINARY:
                this.registerColumnTypeForLongVarbinary(ct, ci);
                break;
            case JdbcDataType.NULL:
                this.registerColumnTypeForNull(ct, ci);
                break;
            case JdbcDataType.OTHER:
                this.registerColumnTypeForOther(ct, ci);
                break;
            case JdbcDataType.JAVA_OBJECT:
                this.registerColumnTypeForJavaObject(ct, ci);
                break;
            case JdbcDataType.DISTINCT:
                this.registerColumnTypeForDistinct(ct, ci);
                break;
            case JdbcDataType.STRUCT:
                this.registerColumnTypeForStruct(ct, ci);
                break;
            case JdbcDataType.ARRAY:
                this.registerColumnTypeForArray(ct, ci);
                break;
            case JdbcDataType.BLOB:
                this.registerColumnTypeForBlob(ct, ci);
                break;
            case JdbcDataType.CLOB:
                this.registerColumnTypeForClob(ct, ci);
                break;
            case JdbcDataType.REF:
                this.registerColumnTypeForRef(ct, ci);
                break;
            case JdbcDataType.DATALINK:
                this.registerColumnTypeForDataLink(ct, ci);
                break;
            case JdbcDataType.BOOLEAN:
                this.registerColumnTypeForBoolean(ct, ci);
                break;
            case JdbcDataType.ROWID:
                this.registerColumnTypeForRowId(ct, ci);
                break;
            case JdbcDataType.NCHAR:
                this.registerColumnTypeForNChar(ct, ci);
                break;
            case JdbcDataType.NVARCHAR:
                this.registerColumnTypeForNVarchar(ct, ci);
                break;
            case JdbcDataType.LONGNVARCHAR:
                this.registerColumnTypeForLongNVarchar(ct, ci);
                break;
            case JdbcDataType.NCLOB:
                this.registerColumnTypeForNClob(ct, ci);
                break;
            case JdbcDataType.SQLXML:
                this.registerColumnTypeForSqlXml(ct, ci);
                break;
            case JdbcDataType.REF_CURSOR:
                this.registerColumnTypeForRefCursor(ct, ci);
                break;
            case JdbcDataType.TIME_WITH_TIMEZONE:
                this.registerColumnTypeForTimeZ(ct, ci);
                break;
            case JdbcDataType.TIMESTAMP_WITH_TIMEZONE:
                this.registerColumnTypeForTimestampZ(ct, ci);
                break;


            // --------------------- sqlserver ---------------------
            case JdbcDataType.DATETIMEOFFSET:
                this.registerColumnTypeForDateTimeOffset(ct, ci);
                break;
            case JdbcDataType.STRUCTURED:
                this.registerColumnTypeForStructured(ct, ci);
                break;
            case JdbcDataType.DATETIME:
                this.registerColumnTypeForDateTime(ct, ci);
                break;
            case JdbcDataType.SMALLDATETIME:
                this.registerColumnTypeForSmallDateTime(ct, ci);
                break;
            case JdbcDataType.MONEY:
                this.registerColumnTypeForMoney(ct, ci);
                break;
            case JdbcDataType.SMALLMONEY:
                this.registerColumnTypeForSmallMoney(ct, ci);
                break;
            case JdbcDataType.GUID:
                this.registerColumnTypeForGuid(ct, ci);
                break;
            case JdbcDataType.SQL_VARIANT:
                this.registerColumnTypeForSqlVariant(ct, ci);
                break;
            case JdbcDataType.GEOMETRY:
                this.registerColumnTypeForGeometry(ct, ci);
                break;
            case JdbcDataType.GEOGRAPHY:
                this.registerColumnTypeForGeography(ct, ci);
                break;
            // --------------------- sqlserver ---------------------

            // --------------------- oracle ---------------------
            case JdbcDataType.JAVA_STRUCT:
                this.registerColumnTypeForJavaStruct(ct, ci);
                break;
            // case JdbcDataType.TIMESTAMPNS:
            //      = this.registerColumnTypeForJavaStruct(ct, ci);
            //     break;
            case JdbcDataType.TIMESTAMPTZ:
                this.registerColumnTypeForTimestampTZ(ct, ci);
                break;
            case JdbcDataType.TIMESTAMPLTZ:
                this.registerColumnTypeForTimestampLTZ(ct, ci);
                break;
            case JdbcDataType.INTERVALYM:
                this.registerColumnTypeForIntervalYM(ct, ci);
                break;
            case JdbcDataType.INTERVALDS:
                this.registerColumnTypeForIntervalDS(ct, ci);
                break;
            case JdbcDataType.CURSOR:
                this.registerColumnTypeForCursor(ct, ci);
                break;
            case JdbcDataType.BFILE:
                this.registerColumnTypeForBfile(ct, ci);
                break;
            case JdbcDataType.OPAQUE:
                this.registerColumnTypeForOpaque(ct, ci);
                break;
            case JdbcDataType.PLSQL_INDEX_TABLE:
                this.registerColumnTypeForPlsqlIndexTable(ct, ci);
                break;
            case JdbcDataType.BINARY_FLOAT:
                this.registerColumnTypeForBinaryFloat(ct, ci);
                break;
            case JdbcDataType.BINARY_DOUBLE:
                this.registerColumnTypeForBinaryDouble(ct, ci);
                break;
            // case JdbcDataType.NUMBER : break;
            // case JdbcDataType.RAW : break;
            case JdbcDataType.FIXED_CHAR:
                this.registerColumnTypeForFixedChar(ct, ci);
                break;
            // --------------------- oracle ---------------------

            // --------------------- mysql ---------------------
            case JdbcDataType.ENUM:
                this.registerColumnTypeForEnum(ct, ci);
                break;
            case JdbcDataType.SET:
                this.registerColumnTypeForSet(ct, ci);
                break;
            case JdbcDataType.JSON:
                this.registerColumnTypeForJson(ct, ci);
                break;
            case JdbcDataType.YEAR:
                this.registerColumnTypeForYear(ct, ci);
                break;
            // --------------------- mysql ---------------------
        }

        return ct;
    }

    public void registerColumnTypeForYear(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForJson(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForSet(ColumnType ct, ColumnInfo ci) {
        ct.setLength(ci.getColumnSize());
    }

    public void registerColumnTypeForEnum(ColumnType ct, ColumnInfo ci) {
        ct.setLength(ci.getColumnSize());
    }

    public void registerColumnTypeForFixedChar(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForBinaryDouble(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForBinaryFloat(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForPlsqlIndexTable(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForOpaque(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForBfile(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForCursor(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForIntervalDS(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForIntervalYM(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForTimestampLTZ(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForTimestampTZ(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForJavaStruct(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForGeography(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForGeometry(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForSqlVariant(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForGuid(ColumnType ct, ColumnInfo ci) {
        ct.setJdbcDataType(JdbcDataType.CHAR);
        ct.setLength(36);
    }

    public void registerColumnTypeForSmallMoney(ColumnType ct, ColumnInfo ci) {
        ct.setPrecision(10);
        ct.setScale(4);
    }

    public void registerColumnTypeForMoney(ColumnType ct, ColumnInfo ci) {
        ct.setPrecision(19);
        ct.setScale(4);
    }

    public void registerColumnTypeForSmallDateTime(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForDateTime(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForStructured(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForDateTimeOffset(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForTimestampZ(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForTimeZ(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForRefCursor(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForSqlXml(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForNClob(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForLongNVarchar(ColumnType ct, ColumnInfo ci) {
        ct.setJdbcDataType(JdbcDataType.LONGNVARCHAR);
    }

    public void registerColumnTypeForNVarchar(ColumnType ct, ColumnInfo ci) {
        // nvarchar
        boolean[] doesIgnoreLength = new boolean[1];
        int jdbcDataType = this.getRightDataTypeForNVarchar(ci.getDataType(), doesIgnoreLength);
        if (!doesIgnoreLength[0]) {
            ct.setLength(ci.getColumnSize());
        }
        ct.setJdbcDataType(jdbcDataType);
    }

    public void registerColumnTypeForNChar(ColumnType ct, ColumnInfo ci) {
        boolean[] doesIgnoreLength = new boolean[1];
        int jdbcDataType = this.getRightDataTypeForNChar(ci.getDataType(), doesIgnoreLength);
        if (!doesIgnoreLength[0]) {
            ct.setLength(ci.getColumnSize());
        }
        ct.setJdbcDataType(jdbcDataType);
    }

    public void registerColumnTypeForRowId(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForBoolean(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForDataLink(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForRef(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForClob(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForBlob(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForArray(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForStruct(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForDistinct(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForJavaObject(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForOther(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForNull(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForLongVarbinary(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForVarbinary(ColumnType ct, ColumnInfo ci) {
        // varbinary
        boolean[] doesIgnoreLength = new boolean[1];
        int jdbcDataType = this.getRightDataTypeForVarBinary(ci.getDataType(), doesIgnoreLength);
        if (!doesIgnoreLength[0]) {
            ct.setLength(ci.getColumnSize());
        }
        ct.setJdbcDataType(jdbcDataType);
    }

    public void registerColumnTypeForBinary(ColumnType ct, ColumnInfo ci) {
        // binary
        boolean[] doesIgnoreLength = new boolean[1];
        int jdbcDataType = this.getRightDataTypeForBinary(ci.getDataType(), doesIgnoreLength);
        if (!doesIgnoreLength[0]) {
            ct.setLength(ci.getColumnSize());
        }
        ct.setJdbcDataType(jdbcDataType);
    }

    public void registerColumnTypeForTimestamp(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForTime(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForDate(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForLongVarchar(ColumnType ct, ColumnInfo ci) {
        // long varchar 不支持设置长度
        ct.setJdbcDataType(JdbcDataType.LONGVARCHAR);
    }

    public void registerColumnTypeForVarchar(ColumnType ct, ColumnInfo ci) {
        // varchar
        boolean[] doesIgnoreLength = new boolean[1];
        int jdbcDataType = this.getRightDataTypeForVarchar(ci.getColumnSize(), doesIgnoreLength);
        if (!doesIgnoreLength[0]) {
            ct.setLength(ci.getColumnSize());
        }
        ct.setJdbcDataType(jdbcDataType);
    }

    public void registerColumnTypeForChar(ColumnType ct, ColumnInfo ci) {
        // char > 255
        boolean[] doesIgnoreLength = new boolean[1];
        int jdbcDataType = this.getRightDataTypeForChar(ci.getColumnSize(), doesIgnoreLength);
        if (!doesIgnoreLength[0]) {
            ct.setLength(ci.getColumnSize());
        }

        ct.setJdbcDataType(jdbcDataType);
    }

    public void registerColumnTypeForDecimal(ColumnType ct, ColumnInfo ci) {
        // decimal
        // 最大精度
        int maximumPrecision = this.getMaximumPrecision(ci.getDataType());
        // 最小精度
        int minimumPrecision = this.getMinimumPrecision(ci.getDataType());
        ct.setPrecision(Math.max(Math.min(ci.getColumnSize(), maximumPrecision), minimumPrecision));

        if (ci.getDecimalDigits() > 0) {
            // 最大刻度
            int maximumScale = this.getMaximumScale(ci.getDataType());
            // 最小刻度
            int minimumScale = this.getMinimumScale(ci.getDataType());
            ct.setScale(Math.max(Math.min(ci.getDecimalDigits(), maximumScale), minimumScale));

            return;
        }

        // 是否支持数值类型转成int类型
        if (this.supportsDecimalToInt()) {
            switch (ct.getPrecision()) {
                case 3:
                    this.registerColumnTypeForTinyint(ct, ci);
                    break;
                case 5:
                    this.registerColumnTypeForSmallint(ct, ci);
                    break;
                case 10:
                    this.registerColumnTypeForInt(ct, ci);
                    break;
                case 19:
                    this.registerColumnTypeForBigInt(ct, ci);
                    break;
            }
        }
    }

    public void registerColumnTypeForNumeric(ColumnType ct, ColumnInfo ci) {
        this.registerColumnTypeForDecimal(ct, ci);
    }

    public void registerColumnTypeForDouble(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForReal(ColumnType ct, ColumnInfo ci) {
        // float(24)
    }

    public void registerColumnTypeForFloat(ColumnType ct, ColumnInfo ci) {
        // float(1 ~24) 4bytes
        // float(25~53) 8bytes
    }

    public void registerColumnTypeForBigInt(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForInt(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForSmallint(ColumnType ct, ColumnInfo ci) {
    }

    public void registerColumnTypeForTinyint(ColumnType ct, ColumnInfo ci) {
    }

    @Override
    public String getDropTableString(String tableName) {
        return "drop table " + this.toLookupName(tableName);
    }

    public void registerColumnTypeForBit(ColumnType ct, ColumnInfo ci) {
        ct.setLength(1);
    }

    protected String getFinalColumnName(Map<String, String> remapColumn, String finalTableName, String name){
        String remap = remapColumn.get(String.format("%s.%s", finalTableName, name));
        return remap == null ? name : remap.substring(finalTableName.length() + 1);
    }

    @Override
    public String buildInsertPreparedSql(TableInfo ti, String finalTableName, Map<String, String> remapColumn) {
        if(remapColumn == null){
            remapColumn = new HashMap<>();
        }
        // 生成sql语句：
        // insert into T (col1, col2, col3, ...) values (?, ?, ?, ...)
        final StringBuilder sqlBuilder = new StringBuilder("insert into ").append(this.toLookupName(finalTableName)).append(" (");
        final StringBuilder sqlPlaceHolderBuilder = new StringBuilder();

        for (ColumnInfo c : ti.getColumns()) {
            this.handleBuildInsertPreparedSql(c, getFinalColumnName(remapColumn, finalTableName, c.getColumnName()), sqlBuilder, sqlPlaceHolderBuilder);
        }

        sqlBuilder.delete(sqlBuilder.length() - 1, sqlBuilder.length()).append(") values (");
        sqlPlaceHolderBuilder.delete(sqlPlaceHolderBuilder.length() - 1, sqlPlaceHolderBuilder.length());
        sqlBuilder.append(sqlPlaceHolderBuilder).append(")");

        return sqlBuilder.toString();
    }

    protected void handleBuildInsertPreparedSql(final ColumnInfo c, String finalColumnName, final StringBuilder sqlBuilder, final StringBuilder sqlPlaceHolderBuilder) {
        sqlBuilder.append(this.toLookupName(finalColumnName)).append(",");
        sqlPlaceHolderBuilder.append("?,");
    }

    @Override
    public String buildSelectPreparedSql(TableInfo ti, Map<String, Object[]> queryCondition) {

        StringBuilder sqlBuilder = new StringBuilder("select");
        for (ColumnInfo ci : ti.getColumns()) {
            sqlBuilder.append(this.toLookupName(ci.getColumnName()));
        }
        sqlBuilder.append(" from ").append(this.toLookupName(ti.getTableName()));
        sqlBuilder.append(" where 1 = 1");

        for (Map.Entry<String, Object[]> en : queryCondition.entrySet()) {
            sqlBuilder.append(" and ").append(this.toLookupName(en.getKey()));
            if (en.getValue().length == 1) {
                sqlBuilder.append(" = ? ");
                continue;
            }

            sqlBuilder.append(" in ( ");
            for (int i = 0, len = en.getValue().length; i < len; i++) {
                sqlBuilder.append("?");
                if (i != len - 1) {
                    sqlBuilder.append(",");
                }
            }
            sqlBuilder.append(")");
        }

        return sqlBuilder.toString();
    }

    @Override
    public boolean supportsRecycleBin() {
        return false;
    }

    @Override
    public boolean supportsDropTable() {
        return true;
    }

    @Override
    public boolean supportsParallelExecute() {
        return true;
    }

    @Override
    public boolean defaultUpperCaseName() {
        return false;
    }

    @Override
    public boolean defaultLowerCaseName() {
        return false;
    }
}
