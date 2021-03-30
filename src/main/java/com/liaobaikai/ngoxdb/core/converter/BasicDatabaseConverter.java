package com.liaobaikai.ngoxdb.core.converter;

import com.liaobaikai.ngoxdb.NgoxDbWorker;
import com.liaobaikai.ngoxdb.ParallelMaster;
import com.liaobaikai.ngoxdb.bean.NgoxDbMaster;
import com.liaobaikai.ngoxdb.core.config.DatabaseConfig;
import com.liaobaikai.ngoxdb.core.dialect.DatabaseDialect;
import com.liaobaikai.ngoxdb.core.entity.NgoxDbRelayLog;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 数据库转换器基类
 *
 * @author baikai.liao
 * @Time 2021-01-28 22:17:26
 */
public abstract class BasicDatabaseConverter extends DataWorker implements DatabaseConverter {

    private NgoxDbMaster ngoxDbMaster;

    /**
     * 源数据库中表名的最大长度
     */
    private int tableNameMaxLength;

    /**
     * 导入的表的行数
     */
    private Map<String, AtomicLong> mapOfTableImport;

    /**
     * 表导入失败的数量
     */
    private AtomicInteger importFailCount;

    /**
     * 日志应用失败的数量
     */
    private AtomicInteger applyLogFailCount;

    /**
     * 转换失败的表
     */
    private List<String> createFailTables;

    /**
     * 刚创建的表
     */
    private List<String> createdTables;

    /**
     * 跳过创建的表
     */
    private List<String> skipCreateTables;

    /**
     * 执行失败的日志
     */
    private List<NgoxDbRelayLog> applyFailLogs;

    // 并行
    private ParallelMaster parallelMaster;

    /**
     * 重新映射表名
     */
    private Map<String, String> remapTable;

    public BasicDatabaseConverter() {
        // 默认实现
    }

    public BasicDatabaseConverter(NgoxDbMaster ngoxDbMaster) {
        this.ngoxDbMaster = ngoxDbMaster;

        this.createFailTables = new ArrayList<>();
        this.skipCreateTables = new ArrayList<>();
        this.createdTables = new ArrayList<>();
        this.applyFailLogs = new ArrayList<>();

        this.mapOfTableImport = new HashMap<>();
        this.parallelMaster = new ParallelMaster();
        this.importFailCount = new AtomicInteger();
        this.applyLogFailCount = new AtomicInteger();

        NgoxDbWorker.getMapOfKeepAliveThreadPoolExecutors().put(this, this.parallelMaster.getThreadPoolExecutor());

        this.initRemap();

    }

    private void initRemap() {
        this.remapTable = new HashMap<>();
        String remapTableString = ngoxDbMaster.getDatabaseConfig().getRemapTable();
        if (remapTableString.length() > 0) {
            String[] remapTableStrings = remapTableString.split("\\s*,\\s*");
            for (String perRemapTableString : remapTableStrings) {
                String[] values = perRemapTableString.split(":", 2);
                remapTable.put(values[0].trim(), values[1].trim());
            }
        }
    }

    /**
     * 获取处理后的名称
     *
     * @param name 源字符串
     * @return 目标字符串
     */
    public String toLookupName(String name) {
        return this.getDatabaseDialect().toLookupName(name);
    }

    @Override
    public NgoxDbMaster getNgoxDbMaster() {
        return ngoxDbMaster;
    }

    @Override
    public DatabaseConfig getDatabaseConfig() {
        return ngoxDbMaster.getDatabaseConfig();
    }

    @Override
    public DatabaseDialect getDatabaseDialect() {
        return ngoxDbMaster.getDatabaseDialect();
    }

    /**
     * 处理异常信息
     *
     * @param e 异常对象
     */
    @Override
    public void handleException(Throwable e) {
        e.printStackTrace();
        // getLogger().error("[{}] {}", this.getDatabaseConfig().getName(), e.getMessage());
    }

    /**
     * 判断数据库厂家是否相同
     *
     * @return boolean
     */
    public boolean isSameDatabaseVendor() {
        if (this.ngoxDbMaster.isMaster()) {
            return false;
        }
        return ngoxDbMaster.getMasterDatabaseVendor().equals(this.getDatabaseVendor());
    }

    @Override
    public List<String> getCreateFailTables() {
        return createFailTables;
    }

    @Override
    public List<String> getCreatedTables() {
        return createdTables;
    }

    @Override
    public List<String> getSkipCreateTables() {
        return skipCreateTables;
    }

    @Override
    public List<NgoxDbRelayLog> getApplyFailLogs() {
        return applyFailLogs;
    }

    @Override
    public void applyLog() {
        // 目标数据库执行
        // 应用元数据信息
        List<NgoxDbRelayLog> list = this.getDatabaseDao().getLogRows(NgoxDbRelayLog.NOT_USED);
        // 去重
        Set<NgoxDbRelayLog> set = this.removeDuplicate(list);

        // 先执行修改表的语句
        List<NgoxDbRelayLog> alterTableLogs = new ArrayList<>();
        List<NgoxDbRelayLog> indexLogs = new ArrayList<>();

        for (NgoxDbRelayLog log : set) {
            switch (log.getLogType()) {
                case NgoxDbRelayLog.TYPE_COMMENT:
                case NgoxDbRelayLog.TYPE_INDEX:
                case NgoxDbRelayLog.TYPE_FULLTEXT_INDEX:
                case NgoxDbRelayLog.TYPE_SPATIAL_INDEX:
                    // 创建索引比较耗时，需要另外创建
                    indexLogs.add(log);
                    break;
                default:
                    alterTableLogs.add(log);
            }

        }

        for (NgoxDbRelayLog relayLog : alterTableLogs) {
            apply(relayLog);
        }

        // 多线程应用日志
        this.getParallelMaster().parallelExecute(indexLogs.size(), (index) -> apply(indexLogs.get(index)));
    }

    protected Set<NgoxDbRelayLog> removeDuplicate(List<NgoxDbRelayLog> list) {

        Set<NgoxDbRelayLog> set = new TreeSet<>(Comparator.comparing(NgoxDbRelayLog::getLogText));
        set.addAll(list);

        return set;
    }

    /**
     * 应用日志
     *
     * @param entity 日志对象
     */
    protected void apply(NgoxDbRelayLog entity) {
        try {
            getDatabaseDao().execute(entity.getLogText());
            getLogger().info("[{}] {}", getDatabaseConfig().getName(), entity.getLogText());
            // 更新状态，是否执行过
            entity.setLogUsed(NgoxDbRelayLog.USED);
            getDatabaseDao().updateLogRows(entity);
        } catch (Exception e) {
            applyLogFailCount.incrementAndGet();
            getLogger().error("[{}] {}", this.getDatabaseConfig().getName(), e.getMessage());
        }
    }

    @Override
    public Map<String, AtomicLong> getMapOfTableImport() {
        return mapOfTableImport;
    }


    @Override
    public int getTableNameMaxLength() {
        return tableNameMaxLength;
    }

    @Override
    public void setTableNameMaxLength(int tableNameMaxLength) {
        this.tableNameMaxLength = tableNameMaxLength;
    }

    @Override
    public ParallelMaster getParallelMaster() {
        return parallelMaster;
    }

    @Override
    public Map<String, String> getRemapTable() {
        return remapTable;
    }

    @Override
    public AtomicInteger getImportFailCount() {
        return importFailCount;
    }

    @Override
    public AtomicInteger getApplyLogFailCount() {
        return applyLogFailCount;
    }
}
