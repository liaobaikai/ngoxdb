// package com.liaobaikai.ngoxdb.service.impl;
//
//
// import com.liaobaikai.ngoxdb.dao.impl.MySQLDatabaseDao;
// import com.liaobaikai.ngoxdb.entity.SlaveMetaDataEntity;
// import com.liaobaikai.ngoxdb.info.TableForeignKeyInfo;
// import com.liaobaikai.ngoxdb.info.TableIndexInfo;
// import com.liaobaikai.ngoxdb.info.TableInfo;
// import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
// import com.liaobaikai.ngoxdb.service.DatabaseConverter;
// import com.liaobaikai.ngoxdb.utils.DateUtils;
// import com.liaobaikai.ngoxdb.utils.PageUtils;
// import com.mysql.cj.MysqlType;
// import lombok.extern.slf4j.Slf4j;
// import org.springframework.beans.factory.annotation.Qualifier;
// import org.springframework.context.annotation.Scope;
// import org.springframework.stereotype.Service;
//
// import java.sql.Types;
// import java.util.LinkedHashMap;
// import java.util.List;
//
// /**
//  * MySQL数据库转换器实现类
//  *
//  * @author baikai.liao
//  * @Time 2021-01-17 23:17:55
//  */
// @Slf4j
// @Service("MySQLConverter")
// @Scope("prototype")
// public class MySQLConverter extends BasicDatabaseConverter {
//
//     private final MySQLDatabaseDao masterMySQLDao;
//     private final MySQLDatabaseDao slaveMySQLDao;
//
//     public MySQLConverter(@Qualifier("masterJdbcTemplate") JdbcTemplate2 masterJdbcTemplate,
//                           @Qualifier("slaveJdbcTemplate") JdbcTemplate2 slaveJdbcTemplate) {
//         super(masterJdbcTemplate, slaveJdbcTemplate);
//
//         this.masterMySQLDao = new MySQLDatabaseDao(masterJdbcTemplate);
//         this.slaveMySQLDao = new MySQLDatabaseDao(slaveJdbcTemplate);
//     }
//
//     @Override
//     public TableInfo getTableInfo(String tableName) {
//
//         // TableInfo tableInfo = this.masterMySQLDao.getTableInfo(tableName);
//
//         // return afterGetTableInfo(tableInfo);
//         return null;
//     }
//
//     @Override
//     public TableInfo afterGetTableInfo(TableInfo tableInfo) {
//         if (tableInfo != null) {
//             tableInfo.setTableColumnInfoList(this.masterMySQLDao.getTableColumnInfo(tableInfo.getTableName()));
//             tableInfo.setTableIndexInfoList(this.masterMySQLDao.getTableIndexInfo(tableInfo.getTableName()));
//             tableInfo.setTableForeignKeyInfoList(this.masterMySQLDao.getTableForeignKeyInfo(tableInfo.getTableName()));
//             tableInfo.setTableCheckConstraintInfoList(this.masterMySQLDao.getTableCheckConstraintInfo(tableInfo.getTableName()));
//
//             // 设置jdbcType
//             tableInfo.getTableColumnInfoList().forEach(tableColumnInfo -> {
//                 MysqlType mysqlType = MysqlType.getByName(tableColumnInfo.getColumnType());
//                 tableColumnInfo.setJdbcType(mysqlType == null ? Types.NULL : mysqlType.getJdbcType());
//
//                 tableColumnInfo.setDefaultValue(DateUtils.convert(
//                         this.masterDatabaseConfig.getDatabase(), this.slaveDatabaseConfig.getDatabase(), tableColumnInfo.getDefaultValue(), tableColumnInfo.getJdbcType()));
//             });
//
//             // 检查约束
//             tableInfo.getTableCheckConstraintInfoList().forEach(checkConstraintInfo -> {
//                 // 处理独有语法
//                 // 如：`actor_id2` > 0 and `actor_id2` < 100
//                 checkConstraintInfo.setCheckClause(checkConstraintInfo.getCheckClause().replaceAll("`", ""));
//             });
//
//         }
//         return tableInfo;
//     }
//
//     @Override
//     public List<TableInfo> getTableInfo() {
//
//         List<TableInfo> tableInfoList = this.masterMySQLDao.getTableInfo();
//
//         tableInfoList.forEach(this::afterGetTableInfo);
//
//         return tableInfoList;
//     }
//
//     @Override
//     public boolean isExistsSlaveMetadata(String tableName) {
//         return this.slaveMySQLDao.countTable(tableName) > 0;
//     }
//
//     @Override
//     public void beforeCreateTable(TableInfo tableInfo) {
//         super.beforeCreateTable(tableInfo);
//
//         // 获取目标的默认字符集
//         // TableExtentInfo tableExtentInfo =
//         //         this.slaveMySQLDao.getTableExtentInfo(tableInfo.getTableName());
//         // tableInfo.setTableExtentInfo(tableExtentInfo);
//
//
//         // 索引
//         tableInfo.getTableIndexInfoMap().forEach((key, list) -> {
//
//             SlaveMetaDataEntity slaveMetadataEntity = new SlaveMetaDataEntity();
//             slaveMetadataEntity.setTableName(tableInfo.getTableName());
//
//             StringBuilder statementBuilder = new StringBuilder();
//
//             for (int i = 0, len = list.size(); i < len; i++) {
//                 TableIndexInfo tableIndexInfo = list.get(i);
//                 if (i == 0) {
//                     if (tableIndexInfo.isClusteredIndex()) {
//                         // 忽略
//                         return;
//                     } else if (tableIndexInfo.isNonUniqueSecondaryIndex()) {
//                         statementBuilder.append("create INDEX");
//                         slaveMetadataEntity.setFlags(SlaveMetaDataEntity.FLAG_INDEX);
//                     } else if (tableIndexInfo.isUniqueNonClusteredIndex()) {
//                         statementBuilder.append("create UNIQUE INDEX");
//                         slaveMetadataEntity.setFlags(SlaveMetaDataEntity.FLAG_UNIQUE_INDEX);
//                     } else if (tableIndexInfo.isFullTextIndex()) {
//                         statementBuilder.append("create FULLTEXT INDEX");
//                         slaveMetadataEntity.setFlags(SlaveMetaDataEntity.FLAG_FULLTEXT_INDEX);
//                     } else {
//                         // 其他类型的索引...
//                     }
//
//                     statementBuilder.append(" `").append(key).append("`")
//                             .append(" ON ")
//                             .append("`")
//                             .append(tableInfo.getTableName())
//                             .append("` (");
//                 }
//
//                 statementBuilder.append("`").append(tableIndexInfo.getColumnName()).append("`, ");
//
//                 if (i == len - 1) {
//                     statementBuilder.delete(statementBuilder.length() - 2, statementBuilder.length());
//                     statementBuilder.append(")");
//                 }
//
//             }
//
//             slaveMetadataEntity.setStatement(statementBuilder.toString());
//             if (statementBuilder.length() > 0) {
//                 // primary
//                 this.insertSlaveMetaData(slaveMetadataEntity);
//             }
//         });
//
//         // 外键
//         tableInfo.getTableForeignKeyInfoList().forEach((foreignKeyInfo) -> {
//             StringBuilder statementBuilder = new StringBuilder("alter table ");
//             statementBuilder.append("`").append(tableInfo.getTableName()).append("`")
//                     .append(" ADD CONSTRAINT ")
//                     .append("`").append(foreignKeyInfo.getName()).append("` ")
//                     .append("FOREIGN KEY (`").append(foreignKeyInfo.getColumnName()).append("`) ")
//                     .append("REFERENCES ").append(foreignKeyInfo.getReferenceSchemaName()).append(".`").append(foreignKeyInfo.getReferenceTableName()).append("` (").append(foreignKeyInfo.getReferenceColumnName()).append(")");
//             // 删除、更新动作
//             // set_default, 不支持
//             TableForeignKeyInfo.ReferentialAction deleteReferentialAction =
//                     TableForeignKeyInfo.ReferentialAction.getByName(foreignKeyInfo.getDeleteReferentialActionDesc());
//             // 不支持设置为默认值
//             if (deleteReferentialAction.equals(TableForeignKeyInfo.ReferentialAction.SET_DEFAULT)) {
//                 deleteReferentialAction = TableForeignKeyInfo.ReferentialAction.RESTRICT;
//             }
//
//             TableForeignKeyInfo.ReferentialAction updateReferentialAction =
//                     TableForeignKeyInfo.ReferentialAction.getByName(foreignKeyInfo.getUpdateReferentialActionDesc());
//             // 不支持设置为默认值
//             if (updateReferentialAction.equals(TableForeignKeyInfo.ReferentialAction.SET_DEFAULT)) {
//                 updateReferentialAction = TableForeignKeyInfo.ReferentialAction.RESTRICT;
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
//             String statementBuilder = "alter table " + "`" + tableInfo.getTableName() + "`" +
//                     " ADD CONSTRAINT " +
//                     "`" + checkConstraintInfo.getConstraintName() + "` " +
//                     "CHECK (" + checkConstraintInfo.getCheckClause() + ") ";
//
//             this.insertSlaveMetaData(null, tableInfo.getTableName(),
//                     SlaveMetaDataEntity.FLAG_CHECK, statementBuilder);
//         });
//     }
//
//     @Override
//     public int createTable(TableInfo tableInfo) {
//         this.beforeCreateTable(tableInfo);
//
//         try {
//             // 如果需要替换表的话，需要删除
//             if(this.slaveMySQLDao.countTable(tableInfo.getTableName()) > 0){
//                 if (this.slaveDatabaseConfig.isTableReplace()) {
//                     this.slaveMySQLDao.dropTable(tableInfo);
//                 } else {
//                     log.info("表{}已存在，已跳过。", tableInfo.getTableName());
//                     return 0;
//                 }
//             }
//
//             this.slaveMySQLDao.createTable(tableInfo);
//
//             // 创建表后的动作
//             this.afterCreateTable(tableInfo);
//
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
//         long total = this.masterMySQLDao.selectCount(tableName);
//         int pageCount = PageUtils.getPageCount(pageSize, total);
//
//         while (pageNum <= pageCount) {
//
//             long begin = System.currentTimeMillis();
//
//             List<LinkedHashMap<String, Object>> rows = this.masterMySQLDao.selectList(tableName, pageNum, pageSize);
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
//         this.slaveMySQLDao.batchInsert(tableName, rows);
//     }
//
//     @Override
//     public void applySlaveMetaData() {
//         // 应用所有未执行的。
//         SlaveMetaDataEntity slaveMetaDataEntity = new SlaveMetaDataEntity();
//         slaveMetaDataEntity.setApply(SlaveMetaDataEntity.NOT_APPLY);
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
//
//         for (SlaveMetaDataEntity entity : slaveMetaDataEntityList) {
//             try {
//                 this.slaveMySQLDao.getJdbcTemplate().execute(entity.getStatement());
//             } catch (Exception e) {
//                 this.handleException(e);
//                 continue;
//             }
//             this.ngoxdbMetaDataDao.updateAfterApply(entity);
//         }
//     }
// }
