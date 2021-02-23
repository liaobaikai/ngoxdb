// package com.liaobaikai.ngoxdb.dao;
//
// import com.alibaba.fastjson.JSON;
// import com.alibaba.fastjson.JSONObject;
// import com.liaobaikai.ngoxdb.info.*;
// import com.liaobaikai.ngoxdb.jdbctemplate.SimpleJdbcTemplate;
// import com.liaobaikai.ngoxdb.rs.Table;
// import com.liaobaikai.ngoxdb.utils.NumberUtils;
// import org.springframework.jdbc.core.ConnectionCallback;
// import org.springframework.jdbc.core.JdbcTemplate;
// import org.springframework.jdbc.support.JdbcUtils;
//
// import java.sql.DatabaseMetaData;
// import java.sql.ResultSet;
// import java.util.ArrayList;
// import java.util.LinkedHashMap;
// import java.util.List;
//
// /**
//  * @author baikai.liao
//  * @Time 2021-01-26 19:35:54
//  */
// public class MSAccessDao extends BasicDatabaseDao {
//
//     public MSAccessDao(SimpleJdbcTemplate jdbcTemplate){
//         super(jdbcTemplate);
//     }
//
//     /**
//      * 获取所有的表
//      *
//      * type:
//      * https://www.access-programmers.co.uk/forums/threads/count-tables-forms-etc.103811/
//      * https://stackoverflow.com/questions/3994956/meaning-of-msysobjects-values-32758-32757-and-3-microsoft-access
//      *
//      * Type   TypeDesc
//      * -32768  Form
//      * -32766  Macro
//      * -32764  Reports
//      * -32761  Module
//      * -32758  Users
//      * -32757  Database Document
//      * -32756  Data Access Pages
//      * 1   Table - Local Access Tables
//      * 2   Access Object - Database
//      * 3   Access Object - Containers
//      * 4   Table - Linked ODBC Tables
//      * 5   Queries
//      * 6   Table - Linked Access Tables
//      * 8   SubDataSheets
//      * @param schemaName 表的所有者
//      * @return
//      */
//     @Override
//     public List<TableInfo> getTableInfo(String schemaName) {
//         return this.getJdbcTemplate().execute((ConnectionCallback<List<TableInfo>>) conn -> {
//
//             DatabaseMetaData metaData = conn.getMetaData();
//             ResultSet rs = metaData.getTables(null, null, null, new String[]{"TABLE"});
//
//             List<TableInfo> resultList = new ArrayList<>();
//
//             int columnCount = rs.getMetaData().getColumnCount();
//             while (rs.next()){
//
//                 JSONObject jsonObject = new JSONObject();
//                 for(int i = 0; i < columnCount; i++){
//                     jsonObject.put(JdbcUtils.lookupColumnName(rs.getMetaData(), i + 1), rs.getObject(i + 1));
//                 }
//
//                 Table table = JSON.toJavaObject(jsonObject, Table.class);
//
//                 TableInfo tableInfo = new TableInfo();
//                 tableInfo.setTableSchema(table.getTableSchem());
//                 tableInfo.setTableName(table.getTableName());
//                 tableInfo.setTableColumnInfoList(this.getTableColumnInfo(schemaName, tableInfo.getTableName()));
//                 resultList.add(tableInfo);
//
//                 this.getTableIndexInfo(schemaName, tableInfo.getTableName());
//             }
//
//             return resultList;
//         });
//     }
//
//     @Override
//     public TableInfo getTableInfo(String schemaName, String tableName) {
//
//         return this.getJdbcTemplate().execute((ConnectionCallback<TableInfo>) conn -> {
//
//             DatabaseMetaData metaData = conn.getMetaData();
//             ResultSet rs = metaData.getTables(null, null, tableName, new String[]{"TABLE"});
//
//             TableInfo tableInfo = new TableInfo();
//
//             int columnCount = rs.getMetaData().getColumnCount();
//             if (rs.next()){
//
//                 JSONObject jsonObject = new JSONObject();
//                 for(int i = 0; i < columnCount; i++){
//                     jsonObject.put(JdbcUtils.lookupColumnName(rs.getMetaData(), i + 1), rs.getObject(i + 1));
//                 }
//
//                 Table table = JSON.toJavaObject(jsonObject, Table.class);
//
//                 tableInfo.setTableSchema(table.getTableSchem());
//                 tableInfo.setTableName(table.getTableName());
//
//                 tableInfo.setTableColumnInfoList(this.getTableColumnInfo(schemaName, tableInfo.getTableName()));
//
//                 this.getTableIndexInfo(schemaName, tableInfo.getTableName());
//             }
//
//             return tableInfo;
//         });
//     }
//
//     @Override
//     public List<TableIndexInfo> getTableIndexInfo(String schemaName, String tableName) {
//
//         return this.getJdbcTemplate().execute((ConnectionCallback<List<TableIndexInfo>>) conn -> {
//
//             DatabaseMetaData metaData = conn.getMetaData();
//             ResultSet rs = metaData.getPrimaryKeys(null, null, tableName);
//
//             List<TableIndexInfo> resultList = new ArrayList<>();
//             while (rs.next()){
//                 TableIndexInfo tableIndexInfo = new TableIndexInfo();
//                 // TABLE_CAT: String
//                 // TABLE_SCHEM: String
//                 // TABLE_NAME: String
//                 // COLUMN_NAME: String
//                 // KEY_SEQ: short
//                 // PK_NAME: String
//                 String tableCat = rs.getString("TABLE_CAT");
//                 String tableSchema = rs.getString("TABLE_SCHEM");
//                 String tableName2 = rs.getString("TABLE_NAME");
//                 String columnName = rs.getString("COLUMN_NAME");
//                 short keySeq = rs.getShort("KEY_SEQ");
//                 String pkName = rs.getString("PK_NAME");
//
//                 System.out.println(tableCat);
//                 System.out.println(tableSchema);
//                 System.out.println(tableName2);
//                 System.out.println(columnName);
//                 System.out.println(keySeq);
//                 System.out.println(pkName);
//
//                 resultList.add(tableIndexInfo);
//             }
//
//             rs = metaData.getImportedKeys(null, null, tableName);
//
//             while (rs.next()){
//                 TableIndexInfo tableIndexInfo = new TableIndexInfo();
//                 // PKTABLE_CAT: String
//                 // PKTABLE_SCHEM: String
//                 // PKTABLE_NAME: String
//                 // PKCOLUMN_NAME: String
//                 // FKTABLE_CAT: String
//                 // FKTABLE_SCHEM: String
//                 // FKTABLE_NAME: String
//                 // FKCOLUMN_NAME: String
//                 // KEY_SEQ: short
//                 // UPDATE_RULE: short
//                 // DELETE_RULE: short
//                 // FK_NAME: String
//                 // PK_NAME: String
//                 // DEFERRABILITY: short
//                 String pktableCat = rs.getString("PKTABLE_CAT");
//                 String pktableSchem = rs.getString("PKTABLE_SCHEM");
//                 String pktableName = rs.getString("PKTABLE_NAME");
//                 String pkcolumnName = rs.getString("PKCOLUMN_NAME");
//
//                 String fktableCat = rs.getString("FKTABLE_CAT");
//                 String fktableSchem = rs.getString("FKTABLE_SCHEM");
//                 String fktableName = rs.getString("FKTABLE_NAME");
//                 String fkcolumnName = rs.getString("FKCOLUMN_NAME");
//
//                 short keySeq = rs.getShort("KEY_SEQ");
//                 short updateRule = rs.getShort("UPDATE_RULE");
//                 short deleteRule = rs.getShort("DELETE_RULE");
//
//                 String fkName = rs.getString("FK_NAME");
//                 String pkName = rs.getString("PK_NAME");
//
//                 short deferrability = rs.getShort("DEFERRABILITY");
//
//                 System.out.println(pktableCat);
//                 System.out.println(pktableSchem);
//                 System.out.println(pktableName);
//                 System.out.println(pkcolumnName);
//
//                 resultList.add(tableIndexInfo);
//             }
//
//             rs = metaData.getIndexInfo(null, null, tableName, false, false);
//
//             while (rs.next()){
//                 TableIndexInfo tableIndexInfo = new TableIndexInfo();
//                 // TABLE_CAT: String
//                 // TABLE_SCHEM: String
//                 // TABLE_NAME: String
//                 // NON_UNIQUE: boolean
//                 // INDEX_QUALIFIER: short
//                 // INDEX_NAME: String
//                 // TYPE: short
//                 // ORDINAL_POSITION: short
//                 // COLUMN_NAME: String
//                 // ASC_OR_DESC: String
//                 // CARDINALITY: long
//                 // PAGES: long
//                 // FILTER_CONDITION: String
//                 String tableCat = rs.getString("TABLE_CAT");
//                 String tableSchema = rs.getString("TABLE_SCHEM");
//                 String tableName2 = rs.getString("TABLE_NAME");
//                 String nonUnique = rs.getString("NON_UNIQUE");
//                 String indexQualifier = rs.getString("INDEX_QUALIFIER");
//                 String indexName = rs.getString("INDEX_NAME");
//                 short type = rs.getShort("TYPE");
//                 short ordinalPosition = rs.getShort("ORDINAL_POSITION");
//                 String columnName = rs.getString("COLUMN_NAME");
//                 String ascOrDesc = rs.getString("ASC_OR_DESC");
//                 long cardinality = rs.getLong("CARDINALITY");
//                 long pages = rs.getLong("PAGES");
//                 String filterCondition = rs.getString("FILTER_CONDITION");
//
//                 System.out.println(tableCat);
//                 System.out.println(tableSchema);
//                 System.out.println(tableName2);
//                 System.out.println(nonUnique);
//                 System.out.println(indexQualifier);
//                 System.out.println(indexName);
//                 System.out.println(type);
//                 System.out.println(ordinalPosition);
//                 System.out.println(columnName);
//                 System.out.println(ascOrDesc);
//                 System.out.println(ascOrDesc);
//                 System.out.println(cardinality);
//                 System.out.println(pages);
//                 System.out.println(filterCondition);
//
//                 resultList.add(tableIndexInfo);
//             }
//
//             return resultList;
//         });
//     }
//
//     @Override
//     public List<TableColumnInfo> getTableColumnInfo(String schemaName, String tableName) {
//         return this.getJdbcTemplate().execute((ConnectionCallback<List<TableColumnInfo>>) conn -> {
//             DatabaseMetaData dbmd = conn.getMetaData();
//             ResultSet rs = dbmd.getColumns(null, null, tableName, null);
//             List<TableColumnInfo> tableColumnInfoList = new ArrayList<>();
//
//             while (rs.next()){
//                 TableColumnInfo tableColumnInfo = new TableColumnInfo();
//
//                 tableColumnInfo.setColumnName(rs.getString("COLUMN_NAME"));
//                 tableColumnInfo.setJdbcType(rs.getInt("DATA_TYPE"));
//                 tableColumnInfo.setColumnType(rs.getString("TYPE_NAME"));
//                 tableColumnInfo.setCharMaxLength((long) rs.getInt("CHAR_OCTET_LENGTH"));
//                 tableColumnInfo.setNumberPrecision(rs.getInt("DECIMAL_DIGITS"));
//                 tableColumnInfo.setNotNull("NO".equals(rs.getString("IS_NULLABLE")));
//                 tableColumnInfo.setColumnComment(rs.getString("REMARKS"));
//                 tableColumnInfo.setDefaultValue(rs.getString("COLUMN_DEF"));
//                 tableColumnInfo.setPrimaryKey("YES".equals(rs.getString("IS_AUTOINCREMENT")));
//
//                 tableColumnInfoList.add(tableColumnInfo);
//             }
//
//             return tableColumnInfoList;
//         });
//     }
//
//     @Override
//     public List<TableForeignKeyInfo> getTableForeignKeyInfo(String schemaName, String tableName) {
//         return null;
//     }
//
//     @Override
//     public List<TableCheckConstraintInfo> getTableCheckConstraintInfo(String schemaName, String tableName) {
//         return null;
//     }
//
//     @Override
//     public long selectCount(String schemaName, String tableName) {
//         List<LinkedHashMap<String, Object>> queryResponse =
//                 this.getJdbcTemplate().query(String.format("select count(0) COUNT from %s", tableName));
//
//         return NumberUtils.toLong(queryResponse.get(0).get("COUNT"));
//     }
//
//     @Override
//     public List<LinkedHashMap<String, Object>> selectList(String schemaName, String tableName, int pageNum, int pageSize) {
//
//         return this.getJdbcTemplate().query(String.format("select * from %s", tableName));
//     }
//
//     @Override
//     public void dropTable(TableInfo tableInfo) {
//
//     }
//
//     @Override
//     public void createTable(TableInfo tableInfo) {
//
//     }
//
//     @Override
//     public int countTable(String schemaName, String tableName) {
//         return 0;
//     }
// }
