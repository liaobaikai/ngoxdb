package com.liaobaikai.ngoxdb.service;


import com.liaobaikai.ngoxdb.config.DatabaseConfig;
import com.liaobaikai.ngoxdb.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.enums.fn.DatabaseFunctionEnum;
import com.liaobaikai.ngoxdb.info.TableInfo;

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
     * 数据库访问
     * @return BasicDatabaseDao
     */
    BasicDatabaseDao getDatabaseDao();

    /**
     * 获取分页语句
     * @param ti 表信息
     * @param offset 偏移量
     * @param limit 每页大小
     * @return sql语句
     */
    String getPaginationSQL(TableInfo ti, int offset, int limit);

    /**
     * 获取数据库厂家
     * @return {@link com.liaobaikai.ngoxdb.enums.DatabaseVendorEnum}
     */
    String getDatabaseVendor();

    /**
     * 主库的数据库厂家信息
     * @return {@link com.liaobaikai.ngoxdb.enums.DatabaseVendorEnum}
     */
    String getMasterDatabaseVendor();

    /**
     * 获取数据库配置信息
     * @return DatabaseConfig
     */
    DatabaseConfig getDatabaseConfig();

    /**
     * 自动增长的关键字或函数，返回null代表不支持
     * @return function/keyword
     */
    default String getAutoincrement(){
        return null;
    }

    /**
     * 是否为主库
     * @return boolean
     */
    boolean isMaster();

    /**
     * 获取TableInfo对象
     *
     * @param tableName  表名
     * @return 表信息
     */
    List<TableInfo> getTableInfo(String... tableName);

    /**
     * 创建所有表
     * @param tis 表信息
     */
    void createAllTable(List<TableInfo> tis);

    /**
     * 创建一个表
     * @param ti 表信息
     */
    void createTable(TableInfo ti);

    /**
     * 获取数据库专属名称，为了防止使用特殊字符，如MySQL: `name`, SQLServer: [name]
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
     * @param masterDataTypeDef 主数据库的时间默认值
     * @param dataType 数据类型
     * @return 对应的时间默认值
     */
    String getDatabaseDataTypeDefault(String masterDataTypeDef, int dataType);

    /**
     * 获取本数据库的每一个字符多少个字节。
     * @return 一个字符多少个字节
     */
    int getCharBytes();

    /**
     * 获取不支持的函数列表
     * @return list
     */
    List<String> getUnsupportedFunctions();

    /**
     * 从源数据库中拉取数据。
     * @param ti 表信息
     * @param slaveDatabaseConverters 目标数据库转换器
     */
    void pullData(TableInfo ti, List<DatabaseConverter> slaveDatabaseConverters);

    /**
     * 迁移数据
     * @param ti 表信息
     */
    void migrateData(TableInfo ti);

    /**
     * 应用元数据信息
     */
    void applySlaveDatabaseMetadata();

    // /**
    //  * 将TableInfo对象转创建表
    //  *
    //  * @param tableInfo 表信息
    //  * @return
    //  */
    // int createTable(TableInfo tableInfo);
    //
    // /**
    //  * 迁移表数据
    //  *
    //  * @param destConverter 目标转换器
    //  * @param tableName     表名
    //  * @return
    //  */
    // int transform(Converter destConverter, String tableName);
    //
    // /**
    //  * 批量插入数据
    //  *
    //  * @param tableName  表名
    //  * @param rows       数据
    //  * @return
    //  */
    // void batchInsert(String tableName, List<LinkedHashMap<String, Object>> rows);
    //
    // /**
    //  * 应用日志，创建元数据（默认值，索引、检查约束、外键）
    //  */
    // void applySlaveMetaData();
}
