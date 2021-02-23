// package com.liaobaikai.ngoxdb.service.impl;
//
// import com.liaobaikai.ngoxdb.config.DatabaseConfig;
// import com.liaobaikai.ngoxdb.dao.NgoxdbMetaDataDao;
// import com.liaobaikai.ngoxdb.entity.SlaveMetaDataEntity;
// import com.liaobaikai.ngoxdb.info.TableInfo;
// import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
// import com.liaobaikai.ngoxdb.service.DatabaseConverter;
// import lombok.extern.slf4j.Slf4j;
// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.beans.factory.annotation.Qualifier;
//
// /**
//  * 数据库转换器基本实现类
//  *
//  * @author baikai.liao
//  * @Time 2021-01-20 22:43:23
//  */
// @Slf4j
// public abstract class BasicDatabaseConverter implements DatabaseConverter {
//
//     @Autowired
//     @Qualifier("masterDatabaseConfig")
//     protected DatabaseConfig masterDatabaseConfig;
//     @Autowired
//     @Qualifier("slaveDatabaseConfig")
//     protected DatabaseConfig slaveDatabaseConfig;
//
//     protected final NgoxdbMetaDataDao ngoxdbMetaDataDao;
//     protected final JdbcTemplate2 masterJdbcTemplate;
//     protected final JdbcTemplate2 slaveJdbcTemplate;
//
//     public BasicDatabaseConverter(JdbcTemplate2 masterJdbcTemplate, JdbcTemplate2 slaveJdbcTemplate) {
//         this.masterJdbcTemplate = masterJdbcTemplate;
//         this.slaveJdbcTemplate = slaveJdbcTemplate;
//         this.ngoxdbMetaDataDao = new NgoxdbMetaDataDao(slaveJdbcTemplate);
//     }
//
//     // 判断元数据表是否存在
//     public abstract boolean isExistsSlaveMetadata(String tableName);
//
//     // 通过表信息后的处理
//     public abstract TableInfo afterGetTableInfo(TableInfo tableInfo);
//
//     /**
//      * 创建表之前的操作
//      *
//      * @param tableInfo 表信息
//      */
//     public void beforeCreateTable(TableInfo tableInfo) {
//
//         if (!this.isExistsSlaveMetadata(NgoxdbMetaDataDao.TABLE_NAME)) {
//             this.ngoxdbMetaDataDao.createTable();
//         } else {
//             this.ngoxdbMetaDataDao.delete(this.slaveDatabaseConfig.getDatabaseName(), tableInfo.getTableName());
//         }
//     }
//
//     /**
//      * 异常捕获
//      *
//      * @param e 异常信息
//      */
//     public void handleException(Exception e) {
//         if (log.isErrorEnabled()) {
//             log.error("执行失败: {}", e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
//         }
//         if (log.isInfoEnabled()) {
//             log.info("已跳过。");
//         }
//     }
//
//     // 创建表后的动作
//     public void afterCreateTable(TableInfo tableInfo){
//     }
//
//     /**
//      * 保存元数据
//      * @param schemaName 表所有者
//      * @param tableName  表名
//      * @param flags      类型
//      * @param statement  语句
//      */
//     public void insertSlaveMetaData(String schemaName,
//                                     String tableName,
//                                     String flags,
//                                     String statement) {
//         this.insertSlaveMetaData(new SlaveMetaDataEntity(schemaName, tableName, flags, statement));
//     }
//
//     /**
//      * 保存元数据
//      * @param entity 实体
//      */
//     public void insertSlaveMetaData(SlaveMetaDataEntity entity) {
//         this.ngoxdbMetaDataDao.insert(entity);
//     }
// }
