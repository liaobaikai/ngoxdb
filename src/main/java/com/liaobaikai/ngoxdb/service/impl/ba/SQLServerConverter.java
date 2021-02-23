// package com.liaobaikai.ngoxdb.service.impl;
//
// import com.liaobaikai.ngoxdb.dao.impl.SQLServerDatabaseDao;
// import com.liaobaikai.ngoxdb.entity.SlaveMetaDataEntity;
// import com.liaobaikai.ngoxdb.info.TableColumnInfo;
// import com.liaobaikai.ngoxdb.info.TableForeignKeyInfo;
// import com.liaobaikai.ngoxdb.info.TableInfo;
// import com.liaobaikai.ngoxdb.service.DatabaseConverter;
// import com.liaobaikai.ngoxdb.types.SQLServerType;
// import com.liaobaikai.ngoxdb.utils.DateUtils;
// import com.liaobaikai.ngoxdb.utils.PageUtils;
// import com.liaobaikai.ngoxdb.utils.SQLServerUtils;
// import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
// import lombok.extern.slf4j.Slf4j;
// import org.apache.commons.lang3.StringUtils;
// import org.springframework.beans.factory.annotation.Qualifier;
// import org.springframework.context.annotation.Scope;
// import org.springframework.stereotype.Service;
//
// import java.util.LinkedHashMap;
// import java.util.List;
//
// /**
//  * SQLServer数据库转换器实现类
//  *
//  * @author baikai.liao
//  * @Time 2021-01-18 21:34:55
//  */
// @Slf4j
// @Service("SQLServerConverter")
// @Scope("prototype")
// public class SQLServerConverter extends BasicDatabaseConverter {
//
//     enum CallableHandles {
//         SP_ADD_EXTENDED_PROPERTY_TABLE("exec sys.sp_addextendedproperty @name=N'MS_Description', @value=N'?', @level0type=N'SCHEMA',@level0name=N'?', @level1type=N'TABLE',@level1name=N'?'"),
//         SP_ADD_EXTENDED_PROPERTY_COLUMN("exec sys.sp_addextendedproperty @name=N'MS_Description', @value=N'?' , @level0type=N'SCHEMA',@level0name=N'?', @level1type=N'TABLE',@level1name=N'?', @level2type=N'COLUMN',@level2name=N'?'"),
//         ;
//
//         private final String proc;
//
//         CallableHandles(String name) {
//             this.proc = name;
//         }
//     }
//
//     private final SQLServerDatabaseDao masterSQLServerDao;
//     private final SQLServerDatabaseDao slaveSQLServerDao;
//
//     public SQLServerConverter(@Qualifier("masterJdbcTemplate") JdbcTemplate2 masterJdbcTemplate,
//                               @Qualifier("slaveJdbcTemplate") JdbcTemplate2 slaveJdbcTemplate) {
//         super(masterJdbcTemplate, slaveJdbcTemplate);
//
//         this.masterSQLServerDao = new SQLServerDatabaseDao(masterJdbcTemplate);
//         this.slaveSQLServerDao = new SQLServerDatabaseDao(slaveJdbcTemplate);
//     }
//
//     @Override
//     public boolean isExistsSlaveMetadata(String tableName) {
//         return this.slaveSQLServerDao.countTable(tableName) > 0;
//     }
//
//     @Override
//     public TableInfo afterGetTableInfo(TableInfo tableInfo) {
//
//         if (tableInfo != null) {
//
//             // 所有列
//             tableInfo.setTableColumnInfoList(this.masterSQLServerDao.getTableColumnInfo(tableInfo.getTableName()));
//             // 索引信息
//             tableInfo.setTableIndexInfoList(this.masterSQLServerDao.getTableIndexInfo(tableInfo.getTableName()));
//             // 外键信息
//             tableInfo.setTableForeignKeyInfoList(this.masterSQLServerDao.getTableForeignKeyInfo(tableInfo.getTableName()));
//             // 检查约束
//             tableInfo.setTableCheckConstraintInfoList(this.masterSQLServerDao.getTableCheckConstraintInfo(tableInfo.getTableName()));
//
//             // 设置jdbcType
//             tableInfo.getTableColumnInfoList().forEach((tableColumnInfo) -> {
//
//                 SQLServerType sqlServerType = SQLServerType.getByName(tableColumnInfo.getDataType());
//                 tableColumnInfo.setJdbcType(sqlServerType.getJdbcType().getIntValue());
//
//                 tableColumnInfo.setDefaultValue(DateUtils.convert(
//                         this.masterDatabaseConfig.getDatabase(), this.slaveDatabaseConfig.getDatabase(), tableColumnInfo.getDefaultValue(), tableColumnInfo.getJdbcType()));
//
//                 // 没有精度不用增加
//                 SQLServerUtils.formatColumnType(sqlServerType, tableColumnInfo);
//
//             });
//
//             // 检查约束
//             tableInfo.getTableCheckConstraintInfoList().forEach(checkConstraintInfo -> {
//                 // 处理独有语法
//                 // 如：
//                 // ([claim_id]>(10) AND [claim_id]<(100))
//                 // ([actor_id]>(0) AND [first_name]='test')
//                 // 1，去掉前后的()
//                 // 2，去掉列的中括号
//                 String checkClause = checkConstraintInfo.getCheckClause();
//                 if (checkClause.charAt(0) == '(') {
//                     checkClause = checkClause.substring(1);
//                 }
//                 if (checkClause.charAt(checkClause.length() - 1) == ')') {
//                     checkClause = checkClause.substring(0, checkClause.length() - 1);
//                 }
//
//                 for (TableColumnInfo columnInfo : tableInfo.getTableColumnInfoList()) {
//                     // 去掉列的中括号
//                     checkClause = checkClause.replaceAll("\\[" + columnInfo.getColumnName() + "]", columnInfo.getColumnName());
//                 }
//
//                 checkConstraintInfo.setCheckClause(checkClause);
//             });
//
//         }
//         return tableInfo;
//     }
//
//     @Override
//     public TableInfo getTableInfo(String tableName) {
//
//         // TableInfo tableInfo = this.masterSQLServerDao.getTableInfo(tableName);
//
//         // return this.afterGetTableInfo(tableInfo);
//         return null;
//     }
//
//     @Override
//     public List<TableInfo> getTableInfo() {
//
//         List<TableInfo> tableInfoList = this.masterSQLServerDao.getTableInfo();
//
//         tableInfoList.forEach(this::afterGetTableInfo);
//
//         return tableInfoList;
//     }
//
//     @Override
//     public void beforeCreateTable(TableInfo tableInfo) {
//
//         super.beforeCreateTable(tableInfo);
//
//         // 添加注释
//         // 表注释
//         if (StringUtils.isNotEmpty(tableInfo.getTableComment())) {
//             this.insertSlaveMetaData(null,
//                     tableInfo.getTableName(),
//                     SlaveMetaDataEntity.FLAG_COMMENT,
//                     String.format(CallableHandles.SP_ADD_EXTENDED_PROPERTY_TABLE.proc.replaceAll("\\?", "%s"),
//                             tableInfo.getTableComment(), "dbo", tableInfo.getTableName()));
//         }
//
//         // 列注释
//         tableInfo.getTableColumnInfoList().forEach(tableColumnInfo -> {
//             if (StringUtils.isNotEmpty(tableColumnInfo.getColumnComment())) {
//                 this.insertSlaveMetaData(null,
//                         tableInfo.getTableName(),
//                         SlaveMetaDataEntity.FLAG_COMMENT,
//                         String.format(CallableHandles.SP_ADD_EXTENDED_PROPERTY_COLUMN.proc.replaceAll("\\?", "%s"),
//                                 tableColumnInfo.getColumnComment(), "dbo", tableInfo.getTableName(), tableColumnInfo.getColumnName()));
//             }
//         });
//
//         // 外键
//         tableInfo.getTableForeignKeyInfoList().forEach((foreignKeyInfo) -> {
//             StringBuilder statementBuilder = new StringBuilder("alter table ");
//             statementBuilder.append(tableInfo.getTableName())
//                     .append(" ADD CONSTRAINT ")
//                     .append(foreignKeyInfo.getName())
//                     .append(" FOREIGN KEY (").append(foreignKeyInfo.getColumnName()).append(")")
//                     .append(" REFERENCES ").append(foreignKeyInfo.getReferenceTableName()).append(" (").append(foreignKeyInfo.getReferenceColumnName()).append(")");
//
//             // 删除、更新动作
//             TableForeignKeyInfo.ReferentialAction deleteReferentialAction =
//                     TableForeignKeyInfo.ReferentialAction.getByName(foreignKeyInfo.getDeleteReferentialActionDesc());
//             // 不支持设置为默认值
//             if (deleteReferentialAction.equals(TableForeignKeyInfo.ReferentialAction.RESTRICT)) {
//                 deleteReferentialAction = TableForeignKeyInfo.ReferentialAction.NO_ACTION;
//             }
//
//             TableForeignKeyInfo.ReferentialAction updateReferentialAction =
//                     TableForeignKeyInfo.ReferentialAction.getByName(foreignKeyInfo.getUpdateReferentialActionDesc());
//             // 不支持设置为默认值
//             if (updateReferentialAction.equals(TableForeignKeyInfo.ReferentialAction.RESTRICT)) {
//                 updateReferentialAction = TableForeignKeyInfo.ReferentialAction.NO_ACTION;
//             }
//
//             statementBuilder.append(" ON DELETE ").append(deleteReferentialAction.getValue());
//             statementBuilder.append(" ON UPDATE ").append(updateReferentialAction.getValue());
//
//             this.insertSlaveMetaData(null, tableInfo.getTableName(),
//                     SlaveMetaDataEntity.FLAG_FOREIGN_KEY, statementBuilder.toString());
//         });
//
//         // 检查约束
//         tableInfo.getTableCheckConstraintInfoList().forEach(checkConstraintInfo -> {
//
//             String statementBuilder = "alter table " + tableInfo.getTableName() +
//                     " ADD CONSTRAINT " + checkConstraintInfo.getConstraintName() +
//                     " CHECK (" + checkConstraintInfo.getCheckClause() + ") ";
//
//             this.insertSlaveMetaData(null, tableInfo.getTableName(),
//                     SlaveMetaDataEntity.FLAG_CHECK, statementBuilder);
//         });
//     }
//
//     @Override
//     public int createTable(TableInfo tableInfo) {
//
//         this.beforeCreateTable(tableInfo);
//
//         try {
//             // 如果需要替换表的话，需要删除
//             if(this.slaveSQLServerDao.countTable(tableInfo.getTableName()) > 0){
//                 if (this.slaveDatabaseConfig.isTableReplace()) {
//                     this.slaveSQLServerDao.dropTable(tableInfo);
//                 } else {
//                     log.info("表{}已存在，已跳过。", tableInfo.getTableName());
//                     return 0;
//                 }
//             }
//             this.slaveSQLServerDao.createTable(tableInfo);
//
//             // 创建表后的动作
//             this.afterCreateTable(tableInfo);
//             return 1;
//         } catch (Exception e) {
//             this.handleException(e);
//         }
//
//         return 0;
//     }
//
//     @Override
//     public int transform(DatabaseConverter destConverter, String tableName) {
//
//         int pageNum = 1;
//         int pageSize = DEFAULT_PAGE_SIZE;
//         long total = this.masterSQLServerDao.selectCount(tableName);
//         int pageCount = PageUtils.getPageCount(pageSize, total);
//
//         while (pageNum <= pageCount) {
//
//             long begin = System.currentTimeMillis();
//
//             List<LinkedHashMap<String, Object>> rows = this.masterSQLServerDao.selectList(tableName, pageNum, pageSize);
//
//             // 迁移数据
//             destConverter.batchInsert(tableName, rows);
//
//             if (log.isInfoEnabled()) {
//                 log.info("已迁移 {} 条记录。耗时 {} 毫秒。", rows.size(), (System.currentTimeMillis() - begin));
//             }
//
//             pageNum++;
//         }
//         return 1;
//     }
//
//     @Override
//     public void batchInsert(String tableName, List<LinkedHashMap<String, Object>> rows) {
//         this.slaveSQLServerDao.batchInsert(tableName, rows);
//     }
//
//     @Override
//     public void applySlaveMetaData() {
//         SlaveMetaDataEntity slaveMetaDataEntity = new SlaveMetaDataEntity();
//         slaveMetaDataEntity.setApply(SlaveMetaDataEntity.NOT_APPLY);
//
//         apply(slaveMetaDataEntity);
//     }
//
//     /**
//      * 应用元数据
//      *
//      * @param slaveMetaDataEntity 查询条件
//      */
//     private void apply(SlaveMetaDataEntity slaveMetaDataEntity) {
//         List<SlaveMetaDataEntity> slaveMetaDataEntityList = this.ngoxdbMetaDataDao.selectList(slaveMetaDataEntity);
//         for (SlaveMetaDataEntity entity : slaveMetaDataEntityList) {
//             try {
//                 this.slaveSQLServerDao.getJdbcTemplate().execute(entity.getStatement());
//             } catch (Exception e) {
//                 this.handleException(e);
//                 continue;
//             }
//
//             this.ngoxdbMetaDataDao.updateAfterApply(entity);
//         }
//     }
// }
