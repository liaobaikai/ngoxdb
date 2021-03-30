package com.liaobaikai.ngoxdb.core.converter;


import com.liaobaikai.ngoxdb.bean.NgoxDbMaster;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.core.comparator.DatabaseComparator;
import com.liaobaikai.ngoxdb.core.config.DatabaseConfig;
import com.liaobaikai.ngoxdb.core.dao.DatabaseDao;
import com.liaobaikai.ngoxdb.core.dialect.DatabaseDialect;
import com.liaobaikai.ngoxdb.core.entity.NgoxDbRelayLog;
import com.liaobaikai.ngoxdb.core.listener.OnExportListener;
import org.slf4j.Logger;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 数据库转换器接口
 *
 * @author baikai.liao
 * @Time 2021-01-17 23:17:30
 */
public interface DatabaseConverter {


    /**
     * 获取logger
     *
     * @return Logger
     */
    Logger getLogger();

    /**
     * 数据库访问
     *
     * @return BasicDatabaseDao
     */
    DatabaseDao getDatabaseDao();

    /**
     * 配置
     *
     * @return
     */
    NgoxDbMaster getNgoxDbMaster();

    /**
     * 数据库配置
     *
     * @return
     */
    DatabaseConfig getDatabaseConfig();

    /**
     * 数据库方言
     *
     * @return
     */
    DatabaseDialect getDatabaseDialect();

    /**
     * 数据库厂家
     *
     * @return
     */
    String getDatabaseVendor();

    /**
     * 数据库方言
     *
     * @return
     */
    Class<? extends DatabaseDialect> getDatabaseDialectClass();

    /**
     * 获取创建失败的表
     *
     * @return 表名列表
     */
    List<String> getCreateFailTables();

    /**
     * 获取已创建的表
     *
     * @return 表名列表
     */
    List<String> getCreatedTables();

    /**
     * 获取已跳过的表
     *
     * @return 表名列表
     */
    List<String> getSkipCreateTables();

    /**
     * 获取应用失败的日志
     *
     * @return 日志列表 {@link NgoxDbRelayLog}
     */
    List<NgoxDbRelayLog> getApplyFailLogs();


    /**
     * 获取TableInfo对象
     *
     * @param tableName 表名
     * @return 表信息
     */
    List<TableInfo> getTableInfo(String... tableName);


    //////////////////////////////////////////////////////////////////////////////////////////
    //              目标数据库创建表
    //////////////////////////////////////////////////////////////////////////////////////////

    /**
     * 创建所有表
     *
     * @param tis 表信息
     */
    void createAll(List<TableInfo> tis);

    /**
     * 创建一个表
     *
     * @param ti 表信息
     */
    void create(TableInfo ti);


    //////////////////////////////////////////////////////////////////////////////////////////
    //              源数据库导出数据
    //////////////////////////////////////////////////////////////////////////////////////////

    /**
     * 获取分页信息
     *
     * @param ti     表信息
     * @param offset 偏移量
     * @param limit  每页大小
     * @return 每行数据
     */
    List<Object[]> pagination(TableInfo ti, int offset, int limit);

    /**
     * 导出所有表的数据
     *
     * @param tis              表信息
     * @param onExportListener 从源端数据库获取数据后的监听
     */
    void exportAll(List<TableInfo> tis, OnExportListener onExportListener);

    /**
     * 导出数据前需要做的动作
     *
     * @param ti 表信息
     */
    void beforeExportRows(TableInfo ti);

    /**
     * 导出数据
     *
     * @param ti               表信息
     * @param onExportListener 从源端数据库获取数据后的监听
     */
    void exportRows(TableInfo ti, OnExportListener onExportListener);

    /**
     * 导出数据后需要做的动作
     *
     * @param ti            表信息
     * @param tablePageSize 表的总行数
     */
    void afterExportRows(TableInfo ti, long tablePageSize);


    //////////////////////////////////////////////////////////////////////////////////////////
    //              目标数据库导入数据
    //////////////////////////////////////////////////////////////////////////////////////////


    // /**
    //  * 获取数据，并返回每一行数据
    //  *
    //  * @param ti 表信息
    //  * @return 每一行数据的值的数组列表
    //  */
    // List<Object[]> queryAllData(TableInfo ti);

    // /**
    //  * 获取数据，并返回每一行数据
    //  *
    //  * @param querySql 查询语句
    //  * @return 每一行数据的值的数组列表
    //  */
    // List<Object[]> queryData(String querySql);

    // /**
    //  * 获取数据，并返回每一行数据
    //  *
    //  * @param querySql    查询语句
    //  * @param paramValues 参数值，可为null
    //  * @return 每一行数据的值的数组列表
    //  */
    // List<Object[]> queryData(String querySql, Object[] paramValues);

    /**
     * 导入数据前需要做的操作。
     *
     * @param con       {@link Connection}
     * @param batchArgs 每行的数据
     * @param ti        表信息
     */
    void beforeImportRows(Connection con, List<Object[]> batchArgs, TableInfo ti);

    /**
     * 导入数据
     *
     * @param batchArgs 每行的数据的值
     * @param ti        表信息
     * @param offset    偏移量
     * @param limit     导入的行数
     */
    void importRows(List<Object[]> batchArgs, TableInfo ti, int offset, int limit);

    /**
     * 导入数据后的操作
     *
     * @param con         {@link Connection}
     * @param beginTime   开始导入的时间
     * @param importCount 导入的行数
     * @param offset      偏移量
     * @param limit       每页大小
     * @param ti          表信息
     */
    void afterImportRows(Connection con, long beginTime, int importCount, int offset, int limit, TableInfo ti);

    /**
     * 应用语句
     */
    void applyLog();

    /**
     * 表的行数
     *
     * @return 表->行数
     */
    Map<String, AtomicLong> getMapOfTableImport();

    /**
     * 表名的最长的字符数
     *
     * @return 数字
     */
    int getTableNameMaxLength();

    /**
     * 获取数据库比较器
     *
     * @return 比较器
     */
    DatabaseComparator getComparator();

    /**
     * 设置当前源数据库的所有表的表名的最大长度
     *
     * @param tableNameMaxLength 表名的最大长度
     */
    void setTableNameMaxLength(int tableNameMaxLength);

    AtomicInteger getImportFailCount();

    AtomicInteger getApplyLogFailCount();
}
