package com.liaobaikai.ngoxdb;

import com.alibaba.fastjson.JSON;
import com.liaobaikai.ngoxdb.bean.ComparisonResult;
import com.liaobaikai.ngoxdb.bean.info.ColumnInfo;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.core.config.ConfigManager;
import com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum;
import com.liaobaikai.ngoxdb.core.listener.OnMigrateTableDataListener;
import com.liaobaikai.ngoxdb.core.service.DatabaseComparator;
import com.liaobaikai.ngoxdb.core.service.DatabaseConverter;
import com.liaobaikai.ngoxdb.utils.CommonUtils;
import com.liaobaikai.ngoxdb.utils.DateUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

import java.text.SimpleDateFormat;
import java.util.*;


// https://docs.oracle.com/cloud/help/zh_CN/analytics-cloud/ACSDS/GUID-33F45B17-782F-4A56-9FA9-7163A3BD79B1.htm#ACSDS-GUID-5080E628-864A-4024-B06A-764AED575909
//
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class BootApplication {

    public static void main(String[] args) {
        SpringApplication.run(BootApplication.class, args);

        DatabaseConverter master = ConfigManager.getMasterDatabaseConverter();
        List<DatabaseConverter> slaves = ConfigManager.getSlaveDatabaseConverters();

        master.getLogger().info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>| Collecting all tables... |<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        List<TableInfo> tis = master.getTableInfo();

        master.getLogger().info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>| Create tables... |<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        slaves.forEach(c -> c.createAllTable(tis));

        // 拉取数据
        master.getLogger().info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>| Migrating data... |<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        master.migrateAllTable(tis, new OnMigrateTableDataListener() {

            @Override
            public void onBeforeMigrateTableData(TableInfo ti, long tableRows, int pageSize) {
                if(tableRows == 0) {
                    return;
                }
                slaves.forEach(slave -> {
                    slave.beforeMigrateTableData(ti);
                    // 截断表
                    if (slave.getDatabaseConfig().isTruncateTable()) {
                        slave.getDatabaseDao().truncateTable(slave.getRightName(ti.getTableName()));
                    }
                });
            }

            @Override
            public void onMigrateTableData(List<Object[]> batchArgs, TableInfo ti, int pageNum) {
                slaves.forEach(slave -> {
                    // 迁移数据
                    slave.postData(batchArgs, ti);
                });
            }

            @Override
            public void onAfterMigrateTableData(TableInfo ti, long tableRows) {
                slaves.forEach(slave -> {
                    // 迁移数据
                    slave.afterMigrateTableData(ti, tableRows);
                });
            }
        });

        // 应用
        master.getLogger().info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>| Create indexes and constraints... |<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        slaves.forEach(slave -> {
            slave.postMetadata();

            if (slave.getConvertFailTableList().size() > 0) {
                System.out.println("###################################################################");
                System.out.println("#                              ERROR                              #");
                System.out.println("###################################################################");
                System.out.println(JSON.toJSONString(slave.getConvertFailTableList()));
            }
        });

        master.getLogger().info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>| Table rows count... |<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        slaves.forEach(slave -> {
            List<Map.Entry<String, Long>> entryList = new ArrayList<>(slave.getTableRowCount().entrySet());
            entryList.sort((o1, o2) -> (int) (o2.getValue() - o1.getValue()));

            for(Map.Entry<String, Long> tableRowCountMap: entryList){
                slave.getLogger().info("[{}] Table: {}, Rows: {}",
                        slave.getDatabaseConfig().getName(),
                        String.format("%-"+master.getTableNameMaxLength()+"s", tableRowCountMap.getKey()),
                        tableRowCountMap.getValue());
            }

        });

        // 比较数据库数据
        // compareSlaveDatabases(tis);

    }

    /**
     * 比较数据
     * <p>
     * BUG:
     * 1, sqlserver uniqueidentifier类型排序不支持。转换成其他数据库时候为char(36)，可支持排序。导致比较数据的时候，会引起数据不一致！
     * 2, 表存在多个主键/多个唯一键的时候，无法进行排序。导致比较数据的时候，会引起数据不一致！
     *
     * @param tis 表信息
     */
    private static void compareSlaveDatabases(List<TableInfo> tis) {

        DatabaseConverter master = ConfigManager.getMasterDatabaseConverter();
        List<DatabaseConverter> slaves = ConfigManager.getSlaveDatabaseConverters();
        if (tis == null) {
            tis = master.getTableInfo();
        }

        // 源数据库获取比较器
        master.getComparator().compareAllTables(tis, (ti, tableRowCount, offset, limit, srcRowArgs) -> slaves.forEach(slave -> {

            long slaveTableRowCount = slave.getDatabaseDao().getTableRowCount(null, slave.getRightName(ti.getTableName()));
            // 表的行数不一致
            if (tableRowCount != slaveTableRowCount) {
                System.out.printf("Table: %s, MasterDatabase: %s[%s], SlaveDatabase: %s[%s] 行数不一致！ %n",
                        ti.getTableName(),
                        master.getDatabaseVendor(),
                        tableRowCount,
                        slave.getDatabaseVendor(), slaveTableRowCount);
                return;
            }

            DatabaseComparator slaveComparator = slave.getComparator();

            if (srcRowArgs.size() == 0) {
                // 行数为0
                ComparisonResult comparisonResult = new ComparisonResult();
                comparisonResult.setTableName(ti.getTableName());
                comparisonResult.setFinishTime(new Date());
                comparisonResult.setSkipped(tableRowCount);
                comparisonResult.setRows(tableRowCount);
                slaveComparator.getComparisonResultList().add(comparisonResult);
                return;
            }

            // 主键或唯一键是否为数字类型的。
            boolean isJdbcDecimal = false;
            String uniqueKeyName = null;
            int uniqueKeyIndex = -1;

            switch (ti.getUniqueKeys().size()) {
                case 0:
                    break;
                case 1:
                    for (int i = 0; i < ti.getColumns().size(); i++) {
                        ColumnInfo columnInfo = ti.getColumns().get(i);
                        if (!ti.getUniqueKeys().get(0).equals(columnInfo.getColumnName())) {
                            continue;
                        }
                        isJdbcDecimal = CommonUtils.isJdbcDecimal(columnInfo.getDataType());
                        uniqueKeyName = columnInfo.getColumnName();
                        uniqueKeyIndex = i;
                    }
                    break;
                default:
                    // 多个主键或唯一键
                    for (String uk : ti.getUniqueKeys()) {
                        for (int i = 0; i < ti.getColumns().size(); i++) {
                            ColumnInfo columnInfo = ti.getColumns().get(i);
                            if (!uk.equals(columnInfo.getColumnName())) {
                                continue;
                            }
                            isJdbcDecimal = CommonUtils.isJdbcDecimal(columnInfo.getDataType());
                            if (isJdbcDecimal) {
                                uniqueKeyIndex = i;
                            }
                            uniqueKeyName = columnInfo.getColumnName();
                        }
                    }
                    break;
            }

            if (DatabaseVendorEnum.isSameDatabaseVendor(slave.getDatabaseVendor(), master.getDatabaseVendor()) || isJdbcDecimal) {
                // 数据库厂家一样、主键/唯一键的类型是数字（无排序规则的问题）
                // 获取目标端对应的行的数据。
                List<Object[]> slaveRowArgs = slave.queryPaginationData(ti, offset, limit);

                // 比较数据
                slaveComparator.compare(ti, srcRowArgs, slaveRowArgs);

            } else {
                // 数据库厂家不一样，且主键、唯一键非数字。
                if (uniqueKeyIndex >= 0 && uniqueKeyName != null) {
                    Object[] values = new Object[srcRowArgs.size()];
                    for (int i = 0, len = values.length; i < len; i++) {
                        values[i] = srcRowArgs.get(i)[uniqueKeyIndex];
                    }

                    List<Object[]> slaveRowArgs = slave.queryInData(ti, uniqueKeyName, values);

                    slaveComparator.compare(ti, srcRowArgs, slaveRowArgs, uniqueKeyIndex);

                } else {
                    // 查询唯一数据的列
                    // 如果第一个列的数据是唯一的话，则可以通过第一列来排序？

                    // 不适合
                    ComparisonResult comparisonResult = new ComparisonResult();
                    comparisonResult.setTableName(ti.getTableName());
                    comparisonResult.setFinishTime(new Date());
                    comparisonResult.setSkipped(tableRowCount);
                    comparisonResult.setRows(tableRowCount);
                    slaveComparator.getComparisonResultList().add(comparisonResult);
                }

            }

        }));


        ////////////////////////////////   ///////////////////////////////////////////
        System.out.println("比较结果：");
        System.out.printf("%14s", "BEGIN_TIME");
        System.out.printf("%10s", "ERRORS");
        System.out.printf("%10s", "DIFFS");
        System.out.printf("%10s", "ROWS");
        System.out.printf("%10s", "DIFF_ROWS");
        System.out.printf("%10s", "PAGES");
        System.out.printf("%10s", "SKIPPED");
        System.out.printf("%14s", "FINISH_TIME");
        System.out.printf("%12s", "SLAVE_NAME");
        System.out.printf("%30s\n", "TABLE");

        slaves.forEach(slave -> {
            slave.getComparator().getComparisonResultList().sort(Comparator.comparingInt(ComparisonResult::getDiffs));
            slave.getComparator().getComparisonResultList().forEach(comparisonResult -> {
                System.out.printf("%14s", new SimpleDateFormat("MM-dd HH:mm:ss").format(comparisonResult.getBeginTime()));
                System.out.printf("%10s", comparisonResult.getErrors());
                System.out.printf("%10s", comparisonResult.getDiffs());
                System.out.printf("%10s", comparisonResult.getRows());
                System.out.printf("%10s", comparisonResult.getDiffRows());
                System.out.printf("%10s", comparisonResult.getPages());
                System.out.printf("%10s", comparisonResult.getSkipped());
                System.out.printf("%14s", DateUtils.format(comparisonResult.getFinishTime().getTime() - comparisonResult.getBeginTime().getTime()));
                System.out.printf("%12s", comparisonResult.getSlaveName());
                System.out.printf("%30s\n", comparisonResult.getTableName());
            });
        });

    }


}
