package com.liaobaikai.ngoxdb;

import com.alibaba.fastjson.JSONArray;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.core.config.ConfigManager;
import com.liaobaikai.ngoxdb.core.converter.DatabaseConverter;
import com.liaobaikai.ngoxdb.utils.DateUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author baikai.liao
 * @Time 2021-03-19 15:16:23
 */
@Slf4j
public class NgoxDbWorker {

    private final static String BRAND = "NGOXDB";

    @Getter
    private final static Map<DatabaseConverter, ThreadPoolExecutor> mapOfKeepAliveThreadPoolExecutors = new HashMap<>();
    private final DatabaseConverter masterDatabaseConverter;
    private final List<DatabaseConverter> slaveDatabaseConverters;
    private List<TableInfo> tis;

    /**
     * 开始时间
     */
    private final long beginTime;

    private interface DatabaseConverterDelegate {
        void onDelegate(DatabaseConverter databaseConverter);
    }

    public NgoxDbWorker() {
        beginTime = System.currentTimeMillis();
        masterDatabaseConverter = ConfigManager.getMasterDatabaseConverter();
        slaveDatabaseConverters = ConfigManager.getSlaveDatabaseConverters();
    }

    private void fetchTableInfo() {
        System.out.println();
        masterDatabaseConverter.getLogger().info("************************************* [{}] Collecting tables *************************************", masterDatabaseConverter.getDatabaseConfig().getName());

        // 获取表名
        String tables = masterDatabaseConverter.getDatabaseConfig().getTables();
        String[] args;
        if (tables != null && tables.length() > 0) {
            args = tables.split(",");
            for (int i = 0; i < args.length; i++) {
                args[i] = args[i].trim();
            }
        } else {
            args = new String[0];
        }
        this.tis = masterDatabaseConverter.getTableInfo(args);
    }

    private void createTables() {
        delegate(databaseConverter -> {
            System.out.println();
            log.info("************************************* [{}] Create tables *************************************", databaseConverter.getDatabaseConfig().getName());
            databaseConverter.createAll(tis);
        });
    }

    private void migrate() {

        // 拉取数据
        System.out.println();
        masterDatabaseConverter.getLogger().info("************************************* [{}] Export & Import data *************************************", masterDatabaseConverter.getDatabaseConfig().getName());
        // masterDatabaseConverter.exportAll(tis, new OnExportListener() {
        //
        //     @Override
        //     public void onBeforeExportAll() {
        //
        //     }
        //
        //     @Override
        //     public void onBeforeExportRows(TableInfo ti, long tableRows, int pageSize) {
        //         if (tableRows == 0) {
        //             return;
        //         }
        //         // delegate(databaseConverter -> databaseConverter.beforeExportRows(ti));
        //     }
        //
        //     @Override
        //     public void onExportRows(List<Object[]> batchArgs, TableInfo ti, int offset, int limit) {
        //         delegate(databaseConverter -> databaseConverter.importRows(batchArgs, ti, offset, limit));
        //     }
        //
        //     @Override
        //     public void onAfterExportAll() {
        //         // 从数据库的使用线程池执行创建索引。
        //         // 应用元数据信息
        //
        //     }
        // });
        masterDatabaseConverter.exportAll(tis, (batchArgs, ti, offset, limit) ->
                delegate(databaseConverter -> {
                    databaseConverter.setTableNameMaxLength(masterDatabaseConverter.getTableNameMaxLength());
                    databaseConverter.importRows(batchArgs, ti, offset, limit);
                }));

        // apply log
        delegate(databaseConverter -> {
            System.out.println();
            log.info("************************************* [{}] Apply logs *************************************", databaseConverter.getDatabaseConfig().getName());
            databaseConverter.applyLog();
        });
    }

    public void doWork() {

        try {

            this.fetchTableInfo();
            this.createTables();
            this.migrate();
            this.report();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void report() {
        delegate(databaseConverter -> {
            System.out.println();
            log.info("************************************* [{}] Report .... *************************************", databaseConverter.getDatabaseConfig().getName());

            log.info("[{}] create suss table list: {}", databaseConverter.getDatabaseConfig().getName(), JSONArray.toJSONString(databaseConverter.getCreatedTables()));
            log.info("[{}] create skip table list: {}", databaseConverter.getDatabaseConfig().getName(), JSONArray.toJSONString(databaseConverter.getSkipCreateTables()));
            log.info("[{}] create fail table list: {}", databaseConverter.getDatabaseConfig().getName(), JSONArray.toJSONString(databaseConverter.getCreateFailTables()));
            log.info("[{}] import row error count: {}", databaseConverter.getDatabaseConfig().getName(), databaseConverter.getImportFailCount().get());
            log.info("[{}] apply logs error count: {}", databaseConverter.getDatabaseConfig().getName(), databaseConverter.getApplyLogFailCount().get());

            reportImportRows(databaseConverter);

        });
    }

    /**
     * 报告每一个表导入的行数
     *
     * @param databaseConverter {@link DatabaseConverter}
     */
    private void reportImportRows(DatabaseConverter databaseConverter) {

        if (databaseConverter.getMapOfTableImport().size() == 0) {
            log.info("[{}] No table import.", databaseConverter.getDatabaseConfig().getName());
            return;
        }

        List<Map.Entry<String, AtomicLong>> entryList = new ArrayList<>(databaseConverter.getMapOfTableImport().entrySet());
        entryList.sort((o1, o2) -> (int) (o2.getValue().longValue() - o1.getValue().longValue()));
        for (Map.Entry<String, AtomicLong> tableRowCountMap : entryList) {
            log.info("[{}] table: {}, rows: {}",
                    databaseConverter.getDatabaseConfig().getName(),
                    String.format("%-" + (masterDatabaseConverter.getTableNameMaxLength() + 2) + "s", databaseConverter.getDatabaseDialect().toLookupName(tableRowCountMap.getKey())),
                    tableRowCountMap.getValue());
        }
    }

    private void delegate(DatabaseConverterDelegate delegate) {
        for (DatabaseConverter databaseConverter : slaveDatabaseConverters) {
            delegate.onDelegate(databaseConverter);
        }
    }

    public void finish() {

        // 停止所有线程池
        for (Map.Entry<DatabaseConverter, ThreadPoolExecutor> en : NgoxDbWorker.mapOfKeepAliveThreadPoolExecutors.entrySet()) {
            if (en.getValue() != null && !en.getValue().isShutdown()) {
                en.getValue().shutdown();
            }
        }
        System.out.println();
        log.info("[{}] Convert finish, cost time: {}", BRAND, DateUtils.format(System.currentTimeMillis() - beginTime));
    }
}
