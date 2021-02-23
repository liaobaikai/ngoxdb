// package com.liaobaikai.ngoxdb.service.impl;
//
// import com.liaobaikai.ngoxdb.dao.MSAccessDatabaseDao;
// import com.liaobaikai.ngoxdb.info.TableInfo;
// import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
// import com.liaobaikai.ngoxdb.service.DatabaseConverter;
// import com.liaobaikai.ngoxdb.utils.PageUtils;
// import lombok.extern.slf4j.Slf4j;
// import org.springframework.beans.factory.annotation.Qualifier;
// import org.springframework.context.annotation.Scope;
// import org.springframework.stereotype.Service;
//
// import java.util.LinkedHashMap;
// import java.util.List;
//
// /**
//  * @author baikai.liao
//  * @Time 2021-01-26 19:31:06
//  */
// @Slf4j
// @Service("MSAccessConverter")
// @Scope("prototype")
// public class MSAccessConverter extends BasicDatabaseConverter {
//
//     private final MSAccessDatabaseDao msAccessDao;
//
//     public MSAccessConverter(@Qualifier("masterJdbcTemplate") JdbcTemplate2 masterJdbcTemplate,
//                              @Qualifier("slaveJdbcTemplate") JdbcTemplate2 slaveJdbcTemplate) {
//         super(masterJdbcTemplate, slaveJdbcTemplate);
//         msAccessDao = new MSAccessDatabaseDao(masterJdbcTemplate);
//     }
//
//     @Override
//     public TableInfo getTableInfo(String tableName) {
//         return null;
//     }
//
//     @Override
//     public List<TableInfo> getTableInfo() {
//         return msAccessDao.getTableInfo();
//     }
//
//     @Override
//     public int createTable(TableInfo tableInfo) {
//         return 0;
//     }
//
//     @Override
//     public int transform(DatabaseConverter destConverter, String tableName) {
//
//         int pageNum = 1;
//         int pageSize = DEFAULT_PAGE_SIZE;
//         long total = this.msAccessDao.selectCount(tableName);
//         int pageCount = PageUtils.getPageCount(pageSize, total);
//
//         while (pageNum <= pageCount) {
//
//             long begin = System.currentTimeMillis();
//
//             List<LinkedHashMap<String, Object>> rows = this.msAccessDao.selectList(tableName, pageNum, pageSize);
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
//
//     }
//
//     @Override
//     public void applySlaveMetaData() {
//
//     }
//
//     @Override
//     public boolean isExistsSlaveMetadata(String tableName) {
//         return false;
//     }
//
//     @Override
//     public TableInfo afterGetTableInfo(TableInfo tableInfo) {
//         return null;
//     }
//
//     @Override
//     public List<com.liaobaikai.ngoxdb.info.TableInfo> getTableInfo(String... tableName) {
//         return null;
//     }
// }
