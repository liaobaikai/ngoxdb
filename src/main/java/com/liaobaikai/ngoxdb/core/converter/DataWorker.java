package com.liaobaikai.ngoxdb.core.converter;

import com.liaobaikai.ngoxdb.bean.info.ColumnInfo;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.core.listener.OnExportListener;
import com.liaobaikai.ngoxdb.core.listener.ParallelCallback;
import com.liaobaikai.ngoxdb.utils.StringUtils;
import org.springframework.jdbc.core.ConnectionCallback;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author baikai.liao
 * @Time 2021-03-18 16:25:38
 */
public abstract class DataWorker extends TableCreator {

    public abstract Map<String, AtomicLong> getMapOfTableImport();
    public abstract AtomicInteger getImportFailCount();

    public class ExportParallelCallback implements ParallelCallback {

        private final List<TableInfo> tis;
        private final OnExportListener onExportListener;

        public ExportParallelCallback(List<TableInfo> tis, OnExportListener onExportListener) {
            this.tis = tis;
            this.onExportListener = onExportListener;
        }

        @Override
        public void callback(int index) {
            this.onDataDump(index);
        }

        private void onDataDump(int curr) {
            exportRows(tis.get(curr), onExportListener);
        }
    }

    private class ExportRowsParallelCallback implements ParallelCallback {

        private final int tablePageCount;
        private final int pageSize;
        private final long tableRowCount;
        private final TableInfo ti;
        private final OnExportListener onExportListener;

        public ExportRowsParallelCallback(int tablePageCount,
                                          int pageSize,
                                          long tableRowCount,
                                          TableInfo ti,
                                          OnExportListener onExportListener) {
            this.tablePageCount = tablePageCount;
            this.pageSize = pageSize;
            this.tableRowCount = tableRowCount;
            this.ti = ti;
            this.onExportListener = onExportListener;
        }

        @Override
        public void callback(int index) {
            this.exportRows(index);
        }

        private void exportRows(int index) {
            // 计算分页参数
            int offset = index * pageSize;
            int limit = index != tablePageCount - 1 ? pageSize : (int) (tableRowCount - (long) offset);

            // 分页查询数据
            List<Object[]> batchArgs = pagination(ti, offset, limit);

            getLogger().info("[{}] table: {}, export: {} rows, limit: {}, offset: {}",
                    getDatabaseConfig().getName(),
                    formatTableName(ti.getTableName()),
                    formatRowCount(batchArgs.size()),
                    formatLimit(limit),
                    formatOffset(offset));

            // 导出数据
            onExportListener.exporting(batchArgs, ti, offset, limit);
        }
    }

    private String formatTableName(String tName) {
        return String.format("%-" + (getTableNameMaxLength() + 2) + "s", getDatabaseDialect().toLookupName(tName));
    }

    private String formatRowCount(int rows) {
        return String.format("%-" + (getDatabaseConfig().getPageSize() + "").length() + "s", rows);
    }

    private String formatOffset(int offset) {
        return String.format("%-10s", offset);
    }

    private String formatLimit(int limit) {
        return String.format("%" + (getDatabaseConfig().getPageSize() + "").length() + "s", limit);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////
    //                          迁移数据
    ////////////////////////////////////////////////////////////////////////////////////////////////

    public void exportAll(List<TableInfo> tis, OnExportListener dataDumpListener) {
        // 导出数据
        // 导出所有数据前的动作
        // onExportListener.onBeforeExportAll();

        // 多线程导出表数据
        //ParallelMaster parallelMaster = new ParallelMaster();
        //parallelMaster.parallelSubmit(tis.size(), new OnParallelExportAllListener(tis, onExportListener));
        //parallelMaster.shutdown();
        for (TableInfo ti : tis) {
            exportRows(ti, dataDumpListener);
        }

        // 导出所有数据前的动作
        // onExportListener.onAfterExportAll();

    }

    /**
     * 获取表的所有列名
     *
     * @param columns 表的列信息
     * @return 字符串数组
     */
    protected static String[] columnToArray(List<ColumnInfo> columns) {
        String[] queryColumns = new String[columns.size()];
        for (int i = 0, len = queryColumns.length; i < len; i++) {
            queryColumns[i] = columns.get(i).getColumnName();
        }
        return queryColumns;
    }

    /**
     * 分页查询
     *
     * @param ti     表信息
     * @param offset 偏移量
     * @param limit  每页大小
     * @return 每行的数据
     */
    public List<Object[]> pagination(TableInfo ti, int offset, int limit) {
        try {

            final String[] queryColumns = columnToArray(ti.getColumns());

            boolean isPrimaryKeyOrder = ti.getUniqueKeys().size() > 0;

            String[] orderKeys = isPrimaryKeyOrder ?
                    StringUtils.toArray(ti.getUniqueKeys()) : this.getDatabaseDialect().getTableOrderKeys(ti.getColumns());

            return this.getDatabaseDao().pagination(ti.getTableName(),
                    queryColumns,
                    orderKeys,
                    isPrimaryKeyOrder,
                    offset,
                    limit);

        } catch (Exception e) {
            this.handleException(e);
        }

        return new ArrayList<>();
    }

    public void exportRows(TableInfo ti, OnExportListener onExportListener) {

        final String table = ti.getTableName();

        // 迁移表的数据
        // 查询原系统的数据
        final int pageSize = this.getDatabaseConfig().getPageSize();
        // 获取表的行数
        final long tableRowCount = this.getDatabaseDao().getTableRowCount(table);
        // dataDumpListener.onBeforeExportRows(ti, tableRowCount, pageSize);

        if (tableRowCount == 0) {
            // 无数据
            getLogger().info("[{}] table: {}, export: 0 rows.", this.getDatabaseConfig().getName(),
                    formatTableName(ti.getTableName()));
            return;
        }

        // 分页插入
        // 计算表的页数
        final int tablePageCount = (int) (tableRowCount / pageSize) + (tableRowCount % pageSize > 0 ? 1 : 0);

        // 并行导出
        // if(this.getDatabaseDialect().supportsParallelExecute()){
        this.getParallelMaster().parallelExecute(tablePageCount,
                new ExportRowsParallelCallback(tablePageCount, pageSize, tableRowCount, ti, onExportListener));
        // } else {
        //     this.getParallelMaster().parallelSubmit(tablePageCount, exportListener);
        // }

    }

    public void beforeExportRows(TableInfo ti) {

        // 如果表是刚创建的话，则不需要清空表信息
        if (!this.getCreatedTables().contains(ti.getTableName())) {
            // 截断表
            if (this.getDatabaseConfig().isTruncateTable()) {
                this.getDatabaseDao().truncateTable(ti.getTableName());
            }
        }
    }

    /**
     * 迁移数据后需要做的动作
     *
     * @param ti            表信息
     * @param tablePageSize 表的总行数
     */
    public void afterExportRows(TableInfo ti, long tablePageSize) {

    }

    private int executeSuccessStmtCount(int[] updateCounts) {
        int count = 0;
        for (int rsp : updateCounts) {
            if (rsp != Statement.EXECUTE_FAILED) {
                count++;
            }
        }
        return count;
    }

    public void importRows(List<Object[]> batchArgs, TableInfo ti, int offset, int limit) {

        if (batchArgs.size() == 0) {
            return;
        }

        String finalTableName = getFinalTableName(ti.getTableName());

        // 判断目标数据库是否存在该表
        if (!this.getCreatedTables().contains(finalTableName)
                && !this.getDatabaseDao().existsTable(finalTableName)) {
            // 不是刚创建的表，且表不存在
            this.getLogger().error("[{}] table {} not exists!", this.getDatabaseConfig().getName(), formatTableName(finalTableName));
            return;
        }

        // 构建插入语句
        String key = this.buildTableNameKey(finalTableName);
        StringBuilder sqlBuilder = new StringBuilder();
        String sql = ti.getMapOfPreparedInsertSql().get(key);
        if (sql == null) {
            sqlBuilder.append(this.getDatabaseDialect().buildInsertPreparedSql(ti, finalTableName, getRemapColumn()));
            ti.getMapOfPreparedInsertSql().put(key, sqlBuilder.toString());
        } else {
            sqlBuilder.append(sql);
        }

        getLogger().info("[{}] table: {}, import: {} rows, limit: {}, offset: {}",
                this.getDatabaseConfig().getName(),
                formatTableName(finalTableName),
                formatRowCount(batchArgs.size()),
                formatLimit(limit),
                formatOffset(offset));


        // sqlserver:
        // 将截断字符串或二进制数据
        // https://support.microsoft.com/en-us/topic/kb4468101-improvement-optional-replacement-for-string-or-binary-data-would-be-truncated-message-with-extended-information-in-sql-server-2016-and-2017-a4279ad6-1d3b-3960-77ef-c82a909f4b89
        // 批量插入

        try {

            // 批量执行
            this.getDatabaseDao().getJdbcTemplate().execute((ConnectionCallback<String>) con -> {

                // 开始时间
                long beginTime = System.currentTimeMillis();

                // 处理行数据
                // 目标数据库不支持的类型需要额外处理。
                beforeImportRows(con, batchArgs, ti);

                con.setAutoCommit(false);
                int[] rsp;
                try (PreparedStatement ps = con.prepareStatement(sqlBuilder.toString())) {
                    // 循环添加行数据
                    for (Object[] rowArgs : batchArgs) {
                        // row
                        for (int j = 0; j < rowArgs.length; j++) {
                            // ColumnType columnType = ti.getColumns().get(j)
                            //         .getMapOfColumnType().get(getDatabaseConfig().getDatabase());
                            if (rowArgs[j] == null) {
                                ps.setNull(j + 1, java.sql.Types.NULL);
                            } else {
                                ps.setObject(j + 1, rowArgs[j]);
                            }
                        }
                        ps.addBatch();
                    }

                    rsp = ps.executeBatch();
                }
                con.setAutoCommit(true);

                afterImportRows(con, beginTime, executeSuccessStmtCount(rsp), offset, limit, ti);

                return null;
            });

            // 排队
            // if(importRowsQueue == null){
            //     importRowsQueue = new ArrayBlockingQueue<>(64);
            // }
            //
            // if(!importRowsQueue.offer(batchArgs)){
            //     getLogger().error("[{}] data cannot be queued!", this.getDatabaseConfig().getName());
            // }
            //
            // while (importRowsQueue.size() > 0) {
            //     List<Object[]> objects = importRowsQueue.poll();
            //     if(objects == null) {
            //         break;
            //     }
            //     this.getDatabaseDao().batchUpdate(sql, objects);
            // }

        } catch (Exception e) {
            getImportFailCount().addAndGet(batchArgs.size());
            this.handleException(e);
        }

    }


    /**
     * 生成一个key，由 数据库厂家 + 表名 组成
     *
     * @param tableName 表名
     * @return 目标字符串
     */
    private String buildTableNameKey(String tableName) {
        return String.format("%s.%s", this.getDatabaseConfig().getDatabase(), tableName);
    }

    /**
     * 禁用自动增长，如果支持的话。
     *
     * @param con       {@link Connection}
     * @param tableName 表名
     */
    protected void disableIdentity(Connection con, String tableName) throws SQLException {
        String stmt = this.getDatabaseDialect().getDisableIdentityString(tableName);
        getLogger().info("[{}] {}", this.getDatabaseConfig().getName(), stmt);
        con.createStatement().execute(stmt);
    }

    /**
     * 启用自动增长，如果支持的话。
     *
     * @param con       {@link Connection}
     * @param tableName 表名
     */
    protected void enableIdentity(Connection con, String tableName) throws SQLException {
        String stmt = this.getDatabaseDialect().getEnableIdentityString(tableName);
        getLogger().info("[{}] {}", this.getDatabaseConfig().getName(), stmt);
        con.createStatement().execute(stmt);
    }

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
    public void afterImportRows(Connection con, long beginTime, int importCount, int offset, int limit, TableInfo ti) {

        String finalTableName = getFinalTableName(ti.getTableName());

        // 表的行数统计
        AtomicLong counter = this.getMapOfTableImport().get(finalTableName);
        if (counter == null) {
            counter = new AtomicLong();
            this.getMapOfTableImport().put(finalTableName, counter);
        }
        counter.addAndGet(importCount);

        getLogger().info("[{}] table: {}, import: {} rows, limit: {}, offset: {}, used time: {}(ms).",
                this.getDatabaseConfig().getName(),
                formatTableName(finalTableName),
                formatRowCount(importCount),
                formatLimit(limit),
                offset,
                String.format("%-4s", System.currentTimeMillis() - beginTime));

        if (this.getDatabaseDialect().supportsDisableIdentity() && ti.isHasAutoIdentity()) {
            // 启用自动增长
            try {
                enableIdentity(con, finalTableName);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 导入数据前的操作
     *
     * @param con       {@link Connection}
     * @param batchArgs 批量插入的数据
     * @param ti        表信息
     */
    public void beforeImportRows(Connection con, List<Object[]> batchArgs, TableInfo ti) {
        if (this.getDatabaseDialect().supportsDisableIdentity() && ti.isHasAutoIdentity()) {
            // 禁用自动增长
            try {
                disableIdentity(con, getFinalTableName(ti.getTableName()));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


}
