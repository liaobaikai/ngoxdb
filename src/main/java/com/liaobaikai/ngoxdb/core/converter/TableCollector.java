package com.liaobaikai.ngoxdb.core.converter;

import com.liaobaikai.ngoxdb.ParallelMaster;
import com.liaobaikai.ngoxdb.bean.info.ColumnInfo;
import com.liaobaikai.ngoxdb.bean.info.ConstraintInfo;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.bean.rs.ImportedKey;
import com.liaobaikai.ngoxdb.core.dao.DatabaseDao;
import com.liaobaikai.ngoxdb.core.entity.NgoxDbRelayLog;
import com.liaobaikai.ngoxdb.utils.StringUtils;
import org.slf4j.Logger;

import java.util.List;

/**
 * 表收集器
 *
 * @author baikai.liao
 * @Time 2021-03-18 16:52:57
 */
public abstract class TableCollector {

    public abstract void setTableNameMaxLength(int tableNameMaxLength);

    public abstract int getTableNameMaxLength();

    public abstract String toLookupName(String src);

    public abstract DatabaseDao getDatabaseDao();

    public abstract Logger getLogger();

    public abstract ParallelMaster getParallelMaster();


    public List<TableInfo> getTableInfo(String... tableName) {

        //  from jdbc
        // 表
        List<TableInfo> tables = this.getDatabaseDao().getTables(tableName);
        tables.removeIf(tableInfo -> tableInfo.getTableName().equalsIgnoreCase(NgoxDbRelayLog.TABLE_NAME));

        if (tables.size() == 0) {
            return tables;
        }

        // 多线程生成表信息
        this.getParallelMaster().parallelExecute(tables.size(), ((index) -> buildTableInfo(tables.get(index))));

        // 外键的类型要一致。
        for(TableInfo ti: tables){

            List<ImportedKey> importedKeys = ti.getImportedKeys();
            if(importedKeys.size() > 0){
                // 存在外键
                // 查看引用表的外键列
                for(ImportedKey importedKey: importedKeys){

                    TableInfo refTableInfo = this.findTableInfo(importedKey.getPkTableName(), tables);
                    if(refTableInfo == null){
                        continue;
                    }
                    ColumnInfo refTableColumnInfo = this.findTableColumnInfo(refTableInfo, importedKey.getPkColumnName());
                    if(refTableColumnInfo == null){
                        continue;
                    }
                    ColumnInfo tableColumnInfo = this.findTableColumnInfo(ti, importedKey.getFkColumnName());
                    if(tableColumnInfo == null){
                        continue;
                    }

                    // 两边的类型需要一致，以refTableColumnInfo为标准
                    if(refTableColumnInfo.getDataType() != tableColumnInfo.getDataType()){
                        tableColumnInfo.setDataType(refTableColumnInfo.getDataType());
                        tableColumnInfo.setTypeName(refTableColumnInfo.getTypeName());
                    }
                }
            }
        }

        return tables;
    }

    /**
     * 组装表信息
     *
     * @param tableInfo {@link TableInfo}
     */
    private void buildTableInfo(final TableInfo tableInfo) {

        final String table = tableInfo.getTableName();

        if (table.length() > this.getTableNameMaxLength()) {
            this.setTableNameMaxLength(table.length());
        }

        getLogger().info("collecting table {}...", this.getDatabaseDao().getDatabaseDialect().toLookupName(table));

        // from jdbc.....
        // 列信息
        // 外键
        tableInfo.setColumns(this.getDatabaseDao().getColumns(table));
        tableInfo.setHasAutoIdentity(this.hasAutoIdentity(tableInfo.getColumns()));

        // 主键
        // 获取唯一键
        tableInfo.setPrimaryKeys(this.getDatabaseDao().getPrimaryKeys(table));
        tableInfo.getPrimaryKeys().forEach(primaryKey -> tableInfo.getUniqueKeys().add(primaryKey.getColumnName()));

        // 索引信息
        tableInfo.setIndexInfo(this.getDatabaseDao().getIndexInfo(table));
        // 其他表引用本表的键（外键）
        tableInfo.setExportedKeys(this.getDatabaseDao().getExportedKeys(table));
        // 本表引用其他表的键（外键）
        tableInfo.setImportedKeys(this.getDatabaseDao().getImportedKeys(table));
        // from jdbc end...

        // 约束
        tableInfo.setConstraintInfo(this.getDatabaseDao().getConstraintInfo(table));
        tableInfo.getConstraintInfo().forEach(constraintInfo -> this.formatConstraintColumnName(constraintInfo, tableInfo.getColumns()));

        // 获取唯一键
        boolean hasUniqueKeys = tableInfo.getUniqueKeys().size() > 0;
        if (!hasUniqueKeys) {
            tableInfo.getIndexInfo().forEach(ii -> {
                if (ii.isTableIndexStatistic() || ii.isNonUnique() || ii.getIndexName() == null) {
                    // 统计信息不需要。
                    return;
                }
                tableInfo.getIndexInfo().forEach(ii2 -> {
                    if (!ii.getIndexName().equals(ii2.getIndexName())) {
                        return;
                    }
                    tableInfo.getUniqueKeys().add(ii2.getColumnName());
                });
            });
        }
    }

    private ColumnInfo findTableColumnInfo(TableInfo ti, String columnName){
        for(ColumnInfo ci: ti.getColumns()){
            if(ci.getColumnName().equalsIgnoreCase(columnName)){
                return ci;
            }
        }
        return null;
    }

    private TableInfo findTableInfo(String tableName, List<TableInfo> tis) {
        for(TableInfo ti: tis) {
            if(ti.getTableName().equalsIgnoreCase(tableName)){
                return ti;
            }
        }
        return null;
    }

    /**
     * 格式化检查约束中的字段的名称
     *
     * @param ci             约束条件
     * @param columnInfoList 所有列
     */
    public void formatConstraintColumnName(ConstraintInfo ci, List<ColumnInfo> columnInfoList) {
        String checkCondition = ci.getCheckCondition();
        // 去掉特有的格式
        for (ColumnInfo columnInfo : columnInfoList) {
            checkCondition = checkCondition.replaceAll(
                    StringUtils.escape(this.toLookupName(columnInfo.getColumnName())),
                    columnInfo.getColumnName());
        }
        ci.setCheckCondition(checkCondition);
    }

    /**
     * 表中含有自增字段
     *
     * @param cis 列信息
     * @return true 有, false 无
     */
    protected boolean hasAutoIdentity(List<ColumnInfo> cis) {
        for (ColumnInfo columnInfo : cis) {
            if (columnInfo.isAutoincrement()) {
                return true;
            }
        }
        return false;
    }

}
