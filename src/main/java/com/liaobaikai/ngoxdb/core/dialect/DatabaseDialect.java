package com.liaobaikai.ngoxdb.core.dialect;

import com.liaobaikai.ngoxdb.bean.ColumnType;
import com.liaobaikai.ngoxdb.bean.PreparedPagination;
import com.liaobaikai.ngoxdb.bean.info.ColumnInfo;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.core.enums.func.DatabaseFunctionEnum;
import com.liaobaikai.ngoxdb.core.func.SQLFunction;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * 数据库方言
 *
 * @author baikai.liao
 * @Time 2021-03-06 22:29:09
 */
public interface DatabaseDialect {

    Map<DatabaseFunctionEnum, SQLFunction[]> getSQLFunctionMap();

    Map<Integer, String> getDataTypeMap();

    /**
     * 左边的符号
     *
     * @return 如：'"'
     */
    char openQuote();

    /**
     * 右边的符号
     *
     * @return 如：'"'
     */
    char closeQuote();

    /**
     * 转换名称
     *
     * @param src 源名称
     * @return 目标名称
     */
    String toLookupName(String src);

    /**
     * 是否支持单独comment命令
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsCommentOn();

    /**
     * 是否支持创建表、修改表的时候，设置注释
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsCommentOnBuildTable();

    /**
     * 表注释语句
     *
     * @return comment on ....
     */
    String getTableCommentString(String tableName, String tableComment);

    /**
     * 列注释语句
     *
     * @return comment on ....
     */
    String getColumnCommentString(String tableName, String columnName, String columnComment);

    /**
     * 是否支持创建表、修改表的时候，设置排序规则
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsCollationOnBuildTable();

    /**
     * 排序规则语句
     *
     * @return collate...
     */
    String getCollationString(String collationName);

    /**
     * 是否支持创建表、修改表的时候，设置字符集
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsCharsetOnBuildTable();

    /**
     * 字符集语句
     *
     * @return charset ...
     */
    String getCharsetString(String charsetName);

    /**
     * 添加外键
     *
     * @param constraintName        外键名
     * @param foreignKey            外键列
     * @param referencedTableName   引用表名
     * @param referencedPrimaryKeys 引用列名
     * @return add constraint...
     */
    String getAddForeignKeyConstraintString(String constraintName, String[] foreignKey, String referencedTableName, String[] referencedPrimaryKeys);

    /**
     * 是否支持删除外键
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsDropForeignKey();

    /**
     * 删除外键的语句
     *
     * @return drop constraint...
     */
    String getDropForeignKeyString();

    /**
     * 获取分页语句
     *
     * @param tableName         表名
     * @param queryColumnNames  查询的列名
     * @param orderColumnNames  排序的列名
     * @param isPrimaryKeyOrder 是否主键排序
     * @param offset            偏移量
     * @param limit             每页大小
     * @return prepared sql
     */
    PreparedPagination getPreparedPagination(String tableName, String[] queryColumnNames, String[] orderColumnNames, boolean isPrimaryKeyOrder, int offset, int limit);

    /**
     * 是否支持分区
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsPartitionBy();

    /**
     * 是否支持创建表和删除表时添加 if exists 关键字
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsIfExistsBeforeTableName();

    /**
     * 数据库对null类型的支持
     *
     * @return null, 如果数据库支持null, 否则返回 ""
     */
    String getNullColumnString();

    /**
     * 是否支持序列
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsSequences();

    /**
     * 创建序列的语句
     *
     * @param sequenceName 序列名
     * @param minvalue     最小值
     * @param maxvalue     最大值
     * @param startWith    开始值
     * @param step         步长
     * @return create sequence ...
     */
    String getCreateSequenceString(String sequenceName, long minvalue, long maxvalue, long startWith, long step);

    /**
     * 删除序列的语句
     *
     * @param sequenceName 序列名称
     * @return drop sequence ...
     */
    String getDropSequenceString(String sequenceName);

    /**
     * 是否支持自增标识符替换列类型
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsIdentityReplaceColumnType();

    /**
     * 是否支持自增列
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsIdentityColumns();

    /**
     * 是否需要自增列必须为主键
     *
     * @return 支持 true, 不支持 false
     */
    boolean doesIdentityMustPrimaryKey();

    /**
     * 自增标识符
     *
     * @return 如：auto_increment, autoincrement
     */
    String getIdentityColumnString();

    /**
     * 是否支持通过命令禁用自动增长
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsDisableIdentity();

    /**
     * 禁用自动增长的命令
     *
     * @param tableName 表名
     * @return 如 disable autoincrement...
     */
    String getDisableIdentityString(String tableName);

    /**
     * 启用自动增长的命令
     *
     * @param tableName 表名
     * @return 如 enable autoincrement...
     */
    String getEnableIdentityString(String tableName);

    /**
     * 是否支持级联删除
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsCascadeDelete();

    /**
     * 是否支持limit关键字
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsLimit();

    /**
     * 转换为boolean类型的字符串
     *
     * @param value        布尔值
     * @param jdbcDataType 类型
     * @return 转换后的字符串
     */
    String toBooleanValueString(Object value, int jdbcDataType);

    /**
     * 是否支持truncate表的操作
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsTruncateTable();

    /**
     * 最大的精度
     *
     * @param jdbcDataType jdbc类型
     * @return 数值
     */
    int getMaximumPrecision(int jdbcDataType);

    /**
     * 最小的精度
     *
     * @param jdbcDataType jdbc类型
     * @return 数值
     */
    int getMinimumPrecision(int jdbcDataType);

    /**
     * 最大的刻度
     *
     * @param jdbcDataType jdbc类型
     * @return 数值
     */
    int getMaximumScale(int jdbcDataType);

    /**
     * 最小的刻度
     *
     * @param jdbcDataType jdbc类型
     * @return 数值
     */
    int getMinimumScale(int jdbcDataType);

    /**
     * 通过列的长度获取正确的数据类型
     *
     * @param columnSize        列长度
     * @param doesIgnoredLength 是否忽略长度，传入的长度为1
     * @return {@link java.sql.Types}
     */
    int getRightDataTypeForVarchar(int columnSize, boolean[] doesIgnoredLength);

    /**
     * 通过列的长度获取正确的数据类型
     *
     * @param columnSize 列长度
     * @return {@link java.sql.Types}
     */
    int getRightDataTypeForNVarchar(int columnSize, boolean[] doesIgnoredLength);

    /**
     * 通过列的长度获取正确的数据类型
     *
     * @param columnSize 列长度
     * @return {@link java.sql.Types}
     */
    int getRightDataTypeForChar(int columnSize, boolean[] doesIgnoredLength);

    /**
     * 通过列的长度获取正确的数据类型
     *
     * @param columnSize 列长度
     * @return {@link java.sql.Types}
     */
    int getRightDataTypeForNChar(int columnSize, boolean[] doesIgnoredLength);

    /**
     * 通过列的长度获取正确的数据类型
     *
     * @param columnSize 列长度
     * @return {@link java.sql.Types}
     */
    int getRightDataTypeForBinary(int columnSize, boolean[] doesIgnoredLength);

    /**
     * 通过列的长度获取正确的数据类型
     *
     * @param columnSize 列长度
     * @return {@link java.sql.Types}
     */
    int getRightDataTypeForVarBinary(int columnSize, boolean[] doesIgnoredLength);

    /**
     * 是否支持numeric/decimal类型转成int
     * tinyint=decimal(3)
     * smallint=decimal(5)
     * int=decimal(10)
     * bigInt=decimal(19)
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsDecimalToInt();

    /**
     * 是否支持年份转日期
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsYearToDate();

    /**
     * 是否支持join
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsJoin();

    /**
     * 将独有的类型转换为通用的jdbc类型
     *
     * @param src 源值
     * @return 转换后的值
     */
    Object getSqlGenericType(Object src) throws SQLException;

    /**
     * 是否支持位图索引
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsBitmapIndex();

    /**
     * 是否支持全文索引
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsFullTextIndex();

    /**
     * 全文索引关键字
     *
     * @return 字符串，如 fulltext, context
     */
    String getFullTextIndexString();

    /**
     * 是否支持空间索引
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsSpatialIndex();

    /**
     * 是否支持聚集索引
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsClusteredIndex();

    /**
     * 是否支持全局分区索引
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsGlobalPartitionIndex();

    /**
     * 是否支持本地分区索引
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsLocalPartitionIndex();

    /**
     * 获取表的可以排序的列名，如clob类型的列不支持
     *
     * @param tableColumns 所有列
     * @return col1, col2, col3, ...
     */
    String[] getTableOrderKeys(List<ColumnInfo> tableColumns);

    /**
     * 获取转换后的列类型
     *
     * @param ci 列信息
     * @return {@link ColumnType}
     */
    ColumnType getColumnType(ColumnInfo ci);

    /**
     * 构建插入语句
     *
     * @param ti 表信息
     * @return sql
     */
    String buildInsertPreparedSql(TableInfo ti, String finalTableName);

    /**
     * 构建查询语句
     *
     * @param ti             表信息
     * @param queryCondition 查询条件
     * @return sql
     */
    String buildSelectPreparedSql(TableInfo ti, Map<String, Object[]> queryCondition);

    /**
     * 是否支持回收站功能
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsRecycleBin();

    /**
     * 是否支持删除表
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsDropTable();

    /**
     * 表的删除语句
     */
    String getDropTableString(String tableName);

    /**
     * 是否支持无符号数字
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsUnsignedDecimal();

    /**
     * 是否支持并发 execute 方式，不支持使用 submit
     *
     * @return 支持 true, 不支持 false
     */
    boolean supportsParallelExecute();

    /**
     * 是否默认为大写的名称
     *
     * @return 是 true, 否 false
     */
    boolean defaultUpperCaseName();

    /**
     * 是否默认为小写的名称
     *
     * @return 是 true, 否 false
     */
    boolean defaultLowerCaseName();

}
