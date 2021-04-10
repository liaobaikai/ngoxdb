package com.liaobaikai.ngoxdb;

import com.alibaba.fastjson.JSONObject;
import com.liaobaikai.ngoxdb.anno.Description;
import com.liaobaikai.ngoxdb.bean.ComparisonResult;
import com.liaobaikai.ngoxdb.bean.info.ColumnInfo;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.core.comparator.DatabaseComparator;
import com.liaobaikai.ngoxdb.core.config.ConfigManager;
import com.liaobaikai.ngoxdb.core.config.DatabaseConfig;
import com.liaobaikai.ngoxdb.core.converter.DatabaseConverter;
import com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum;
import com.liaobaikai.ngoxdb.utils.CommonUtils;
import com.liaobaikai.ngoxdb.utils.DateUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.*;


// https://docs.oracle.com/cloud/help/zh_CN/analytics-cloud/ACSDS/GUID-33F45B17-782F-4A56-9FA9-7163A3BD79B1.htm#ACSDS-GUID-5080E628-864A-4024-B06A-764AED575909
//
@Slf4j
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class BootApplication {

    public static void main(String[] args) {

        if (!parseArguments(args)) {
            return;
        }

        SpringApplication springApplication = new SpringApplication(BootApplication.class);
        springApplication.setBannerMode(Banner.Mode.OFF);
        springApplication.run(args);

        NgoxDbWorker ngoxDbWorker = new NgoxDbWorker();
        ngoxDbWorker.doWork();
        ngoxDbWorker.finish();

        // 比较数据库数据
        // compareSlaveDatabases(tis);

    }

    /**
     * 解析参数
     *
     * @param args 参数
     * @return 是否继续下一步
     */
    private static boolean parseArguments(String[] args) {

        for (String arg : args) {
            switch (arg) {
                case "--help":
                case "-h":
                    showHelpDoc();
                    return false;
                case "-v":
                case "--version":
                    showVersion();
                    break;
            }
        }

        return true;
    }

    /**
     * 展示帮助文档
     */
    public static void showHelpDoc() {

        // 获取所有的参数
        System.out.println("主库参数:");
        Field[] fields = DatabaseConfig.class.getDeclaredFields();
        for (Field field : fields) {
            Description annotation = field.getAnnotation(Description.class);
            if (annotation != null) {
                if (!annotation.masterParam()) {
                    continue;
                }
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append(String.format("%-32s %s", "--master." + annotation.name(), annotation.label()));
                printUsage(annotation, stringBuilder);
            }

        }
        System.out.println("\n从库参数:");
        /// slave ..........
        Field[] slaveFields = DatabaseConfig.class.getDeclaredFields();
        for (Field field : slaveFields) {
            Description annotation = field.getAnnotation(Description.class);
            if (annotation != null) {
                if (!annotation.slaveParam()) {
                    continue;
                }
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append(String.format("%-32s %s", "--slave." + annotation.name(), annotation.label()));
                stringBuilder.append(String.format("\n%-32s 别名: --slave.0.%s", "", annotation.name()));

                printUsage(annotation, stringBuilder);
            }

        }

        System.out.println("\n以下写法的参数可支持多个同步从库，数字从0开始");
        /// slave ..........
        Field[] slaveArrFields = DatabaseConfig.class.getDeclaredFields();
        for (Field field : slaveArrFields) {
            Description annotation = field.getAnnotation(Description.class);
            if (annotation != null) {
                if (!annotation.slaveParam()) {
                    continue;
                }
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append(String.format("%-32s %s", "--slave.0." + annotation.name(), annotation.label()));

                printUsage(annotation, stringBuilder);
            }

        }

        System.out.println("\n");

    }

    private static void printUsage(Description annotation, StringBuilder stringBuilder) {
        if (annotation.applyOn().length > 0) {
            stringBuilder.append(", applyOn=(");
            for (DatabaseVendorEnum vendorEnum : annotation.applyOn()) {
                stringBuilder.append(vendorEnum.getVendor()).append(",");
            }
            stringBuilder.delete(stringBuilder.length() - 1, stringBuilder.length()).append(")");
        }

        if (annotation.defaultValue().length() > 0) {
            stringBuilder.append(String.format("\n%32s", "")).append(" 默认值=").append(annotation.defaultValue());
        }
        if (annotation.name().equalsIgnoreCase("database")) {
            // 数据库厂家
            stringBuilder.append(String.format("\n%33s", ""));
            stringBuilder.append("支持的数据库: ");
            for (DatabaseVendorEnum databaseVendorEnum : DatabaseVendorEnum.values()) {
                stringBuilder.append(databaseVendorEnum.getVendor()).append(",");
            }
            stringBuilder.delete(stringBuilder.length() - 1, stringBuilder.length());
        }
        System.out.println(stringBuilder);
    }

    /**
     * 打印版本号
     */
    private static void showVersion() {
        System.out.println("Ngoxdb: version: 1.1.0");
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

            long slaveTableRowCount = slave.getDatabaseDao().getTableRowCount(ti.getTableName());
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
                List<Object[]> slaveRowArgs = slave.pagination(ti, offset, limit);

                // 比较数据
                slaveComparator.compare(ti, srcRowArgs, slaveRowArgs);

            } else {
                // 数据库厂家不一样，且主键、唯一键非数字。
                if (uniqueKeyIndex >= 0 && uniqueKeyName != null) {
                    Object[] values = new Object[srcRowArgs.size()];
                    for (int i = 0, len = values.length; i < len; i++) {
                        values[i] = srcRowArgs.get(i)[uniqueKeyIndex];
                    }

                    Map<String, Object[]> queryCondition = new HashMap<>();
                    queryCondition.put(uniqueKeyName, values);
                    String preparedSql = slave.getDatabaseDialect().buildSelectPreparedSql(ti, queryCondition);
                    List<Object[]> slaveRowArgs = slave.getDatabaseDao().query(preparedSql, values);

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
