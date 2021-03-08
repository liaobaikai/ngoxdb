package com.liaobaikai.ngoxdb.core.service;


import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.core.config.DatabaseConfig;
import com.liaobaikai.ngoxdb.core.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.core.enums.fn.DatabaseFunctionEnum;
import com.liaobaikai.ngoxdb.core.listener.OnMigrateTableDataListener;
import org.slf4j.Logger;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * 数据库转换器接口
 *
 * @author baikai.liao
 * @Time 2021-01-17 23:17:30
 */
public interface DatabaseConverter {

    /**
     * 获取logger
     * @return Logger
     */
    Logger getLogger();

    /**
     * 数据库访问
     *
     * @return BasicDatabaseDao
     */
    BasicDatabaseDao getDatabaseDao();

    /**
     * 获取分页语句
     *
     * @param ti     表信息
     * @param offset 偏移量
     * @param limit  每页大小
     * @return PaginationSQL
     */
    String getPaginationSQL(TableInfo ti, int offset, int limit);

    /**
     * 获取数据库厂家
     *
     * @return {@link com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum}
     */
    String getDatabaseVendor();

    /**
     * 主库的数据库厂家信息
     *
     * @return {@link com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum}
     */
    String getMasterDatabaseVendor();

    /**
     * 获取数据库配置信息
     *
     * @return DatabaseConfig
     */
    DatabaseConfig getDatabaseConfig();

    /**
     * 自动增长的关键字或函数，返回null代表不支持
     *
     * @return function/keyword
     */
    default String getAutoincrement() {
        return null;
    }

    /**
     * 是否为主库
     *
     * @return boolean
     */
    boolean isMaster();

    /**
     * 获取TableInfo对象
     *
     * @param tableName 表名
     * @return 表信息
     */
    List<TableInfo> getTableInfo(String... tableName);

    /**
     * 创建所有表
     *
     * @param tis 表信息
     */
    void createAllTable(List<TableInfo> tis);

    /**
     * 创建一个表
     *
     * @param ti 表信息
     */
    void createTable(TableInfo ti);

    /**
     * 获取数据库专属名称，为了防止使用特殊字符，如MySQL: `name`, SQLServer: [name]
     *
     * @param name char
     * @return name
     */
    String getRightName(String name);

    // /**
    //  * 时间默认值
    //  * @return Map<DateTypeEnum, String[]>
    //  */
    // Map<DateTypeEnum, String[]> getDateTypeDefaults();
    Map<DatabaseFunctionEnum, String[]> getDatabaseFunctionMap();

    /**
     * 获取时间默认值
     *
     * @param masterDataTypeDef 主数据库的时间默认值
     * @param dataType          数据类型
     * @return 对应的时间默认值
     */
    String getDatabaseDataTypeDefault(String masterDataTypeDef, int dataType);

    /**
     * 获取本数据库的每一个字符多少个字节。
     *
     * @return 一个字符多少个字节
     */
    int getCharBytes();

    /**
     * 获取不支持的函数列表
     *
     * @return list
     */
    List<String> getUnsupportedFunctions();

    /**
     * 迁移所有表的数据
     *
     * @param tis                      表信息
     * @param migrateTableDataListener 从源端数据库获取数据后的监听
     */
    void migrateAllTable(List<TableInfo> tis,
                         OnMigrateTableDataListener migrateTableDataListener);

    /**
     * 迁移数据前需要做的动作
     * @param ti 表信息
     */
    void beforeMigrateTableData(TableInfo ti);


    /**
     * 迁移数据
     *
     * @param ti                       表信息
     * @param migrateTableDataListener 从源端数据库获取数据后的监听
     */
    void migrateTableData(TableInfo ti,
                          OnMigrateTableDataListener migrateTableDataListener);

    /**
     * 迁移数据后需要做的动作
     * @param ti 表信息
     * @param tablePageSize 表的总行数
     */
    void afterMigrateTableData(TableInfo ti, long tablePageSize);

    /**
     * 获取数据，并返回每一行数据
     *
     * @param ti 表信息
     * @return 每一行数据的值的数组列表
     */
    List<Object[]> queryAllData(TableInfo ti);

    /**
     * 获取数据，并返回每一行数据
     *
     * @param querySql 查询语句
     * @return 每一行数据的值的数组列表
     */
    List<Object[]> queryData(String querySql);

    /**
     * 获取数据，并返回每一行数据
     *
     * @param querySql    查询语句
     * @param paramValues 参数值，可为null
     * @return 每一行数据的值的数组列表
     */
    List<Object[]> queryData(String querySql, Object[] paramValues);

    /**
     * 分页查询
     *
     * @param ti     表信息
     * @param offset 偏移量
     * @param limit  每页大小
     * @return 每一行数据的值的数组列表
     */
    List<Object[]> queryPaginationData(TableInfo ti, int offset, int limit);

    /**
     * 查询具体的数据
     *
     * @param ti         表信息
     * @param columnName 列名
     * @param inputData  查询的参数值
     * @return 每一行数据的值的数组列表
     */
    List<Object[]> queryInData(TableInfo ti, String columnName, Object[] inputData);

    /**
     * 数据迁移
     *
     * @param batchArgs 每行的数据的值
     * @param ti        表信息
     */
    void postData(List<Object[]> batchArgs, TableInfo ti);

    /**
     * 应用元数据信息
     */
    void postMetadata();

    /**
     * 处理传入的参数。
     *
     * @param batchArgs 每行的数据
     * @param ti        表信息
     */
    void changeBatchArgs(List<Object[]> batchArgs, TableInfo ti);

    /**
     * 将数据保存到数据库
     *
     * @param sql       insert into table (col1, col2, col3,...) values (?, ?, ?,...)
     * @param batchArgs 每行的数据
     * @param ti        表信息
     */
    void persistent(String sql, List<Object[]> batchArgs, TableInfo ti);

    /**
     * 适配数据，从数据库查出来，需要将其转换为其他数据库可以识别的类型（通用的类型）
     *
     * @param srcObj 源数据对象
     * @return 目标对象
     * @throws SQLException sql错误
     */
    Object convertData(Object srcObj) throws SQLException;

    /**
     * 转换失败的表名列表
     *
     * @return 列表
     */
    List<String> getConvertFailTableList();

    /**
     * 表的行数
     * @return 表->行数
     */
    Map<String, Long> getTableRowCount();

    /**
     * 表名的最长的字符数
     * @return 数字
     */
    int getTableNameMaxLength();

    /**
     * 获取数据库比较器
     *
     * @return 比较器
     */
    DatabaseComparator getComparator();

}
