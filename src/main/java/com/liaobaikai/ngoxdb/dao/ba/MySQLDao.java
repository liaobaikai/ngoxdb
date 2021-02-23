// package com.liaobaikai.ngoxdb.dao;
//
// import com.alibaba.fastjson.JSON;
// import com.alibaba.fastjson.JSONObject;
// import com.alibaba.fastjson.TypeReference;
// import com.liaobaikai.ngoxdb.info.*;
// import com.liaobaikai.ngoxdb.jdbctemplate.SimpleJdbcTemplate;
// import com.liaobaikai.ngoxdb.utils.NumberUtils;
// import org.apache.commons.lang3.StringUtils;
// import org.springframework.util.CollectionUtils;
//
// import java.util.LinkedHashMap;
// import java.util.List;
//
// /**
//  * MySQL Mapper
//  *
//  * @author baikai.liao
//  * @Time 2021-01-22 23:54:06
//  */
// public class MySQLDao extends BasicDatabaseDao {
//
//     public MySQLDao(SimpleJdbcTemplate jdbcTemplate){
//         super(jdbcTemplate);
//     }
//
//     @Override
//     public List<TableInfo> getTableInfo(String schemaName) {
//
//         List<LinkedHashMap<String, Object>> queryResponse = this.getJdbcTemplate().query("select\n" +
//                 "            t.TABLE_SCHEMA,\n" +
//                 "            t.TABLE_NAME,\n" +
//                 "            t.AUTO_INCREMENT currentIncrementValue,\n" +
//                 "            t.TABLE_COMMENT\n" +
//                 "        from information_schema.TABLES t\n" +
//                 "        where t.TABLE_SCHEMA = ?\n" +
//                 "            and t.TABLE_TYPE = 'BASE TABLE'\n" +
//                 "            and t.TEMPORARY = 'N'\n" +
//                 "            limit 1", schemaName);
//
//         return JSON.parseObject(JSONObject.toJSONString(queryResponse), new TypeReference<List<TableInfo>>() {
//         });
//     }
//
//     /**
//      * 获取表的基本信息
//      *
//      * @param schemaName 表所有者
//      * @param tableName  表名
//      * @return
//      */
//     @Override
//     public TableInfo getTableInfo(String schemaName, String tableName) {
//
//         List<LinkedHashMap<String, Object>> queryResponse = this.getJdbcTemplate().query("select\n" +
//                 "            t.TABLE_SCHEMA,\n" +
//                 "            t.TABLE_NAME,\n" +
//                 "            t.AUTO_INCREMENT currentIncrementValue,\n" +
//                 "            t.TABLE_COMMENT\n" +
//                 "        from information_schema.TABLES t\n" +
//                 "        where t.TABLE_SCHEMA = ?\n" +
//                 "            and t.TABLE_NAME = ?\n" +
//                 "            and t.TABLE_TYPE = 'BASE TABLE'\n" +
//                 "            and t.TEMPORARY = 'N'\n" +
//                 "            limit 1", schemaName, tableName);
//
//         if (queryResponse.size() == 0) {
//             return null;
//         }
//
//         return JSONObject.toJavaObject((JSON) JSONObject.toJSON(queryResponse.get(0)), TableInfo.class);
//     }
//
//     /**
//      * 获取表的索引信息
//      *
//      * @param schemaName 表所有者
//      * @param tableName  表名
//      * @return
//      */
//     @Override
//     public List<TableIndexInfo> getTableIndexInfo(String schemaName, String tableName) {
//
//         List<LinkedHashMap<String, Object>> queryResponse = this.getJdbcTemplate().query("select\n" +
//                 "            isi.INDEX_ID,\n" +
//                 "            isi.NAME indexName,\n" +
//                 "            case isi.TYPE when 0 then 1 else 0 end isNonUniqueSecondaryIndex,\n" +
//                 "            case isi.TYPE when 1 then 1 else 0 end isAutoGenClusteredIndex,\n" +
//                 "            case isi.TYPE when 2 then 1 else 0 end isUniqueNonClusteredIndex,\n" +
//                 "            case isi.TYPE when 3 then 1 else 0 end isClusteredIndex,\n" +
//                 "            case isi.TYPE when 32 then 1 else 0 end isFullTextIndex,\n" +
//                 "            case isi.TYPE when 64 then 1 else 0 end isSpatialIndex,\n" +
//                 "            case isi.TYPE when 128 then 1 else 0 end isVirtualColumnSecondaryIndex,\n" +
//                 "            isi.N_FIELDS columnCount,\n" +
//                 "            isf.NAME columnName,\n" +
//                 "            isf.POS columnPosition\n" +
//                 "        from information_schema.INNODB_SYS_INDEXES isi\n" +
//                 "                 left join information_schema.INNODB_SYS_TABLES ist\n" +
//                 "                           on isi.TABLE_ID = ist.TABLE_ID\n" +
//                 "                 left join information_schema.INNODB_SYS_FIELDS isf\n" +
//                 "                           on isf.INDEX_ID = isi.INDEX_ID\n" +
//                 "        where ist.NAME = concat(?, '/', ?)", schemaName, tableName);
//
//         return JSON.parseObject(JSONObject.toJSONString(queryResponse), new TypeReference<List<TableIndexInfo>>() {
//         });
//     }
//
//     /**
//      * 获取表的列
//      *
//      * @param schemaName 表所有者
//      * @param tableName  表名
//      * @return
//      */
//     @Override
//     public List<TableColumnInfo> getTableColumnInfo(String schemaName, String tableName) {
//
//         List<LinkedHashMap<String, Object>> queryResponse = this.getJdbcTemplate().query("select\n" +
//                 "            c.TABLE_SCHEMA,\n" +
//                 "            c.TABLE_NAME,\n" +
//                 "            c.COLUMN_NAME,\n" +
//                 "            c.COLUMN_DEFAULT defaultValue,\n" +
//                 "            case c.IS_NULLABLE when 'YES' then 0 else 1 end isNotNull,\n" +
//                 "            c.DATA_TYPE,\n" +
//                 "            c.CHARACTER_MAXIMUM_LENGTH charMaxLength,\n" +
//                 "            c.CHARACTER_OCTET_LENGTH charBytesMaxLength,\n" +
//                 "            c.NUMERIC_PRECISION numberPrecision,\n" +
//                 "            c.NUMERIC_SCALE numberScale,\n" +
//                 "            c.DATETIME_PRECISION,\n" +
//                 "            c.CHARACTER_SET_NAME charsetName,\n" +
//                 "            c.COLLATION_NAME,\n" +
//                 "            c.COLUMN_TYPE,\n" +
//                 "            case c.COLUMN_KEY when 'PRI' then 1 else 0 end isPrimaryKey,\n" +
//                 "            case c.COLUMN_KEY when 'UNI' then 1 else 0 end isUniqueKey,\n" +
//                 "            c.EXTRA,\n" +
//                 "            c.PRIVILEGES,\n" +
//                 "            c.COLUMN_COMMENT\n" +
//                 "        from information_schema.COLUMNS c\n" +
//                 "            left join information_schema.CHECK_CONSTRAINTS cc\n" +
//                 "                on c.TABLE_SCHEMA = cc.CONSTRAINT_SCHEMA\n" +
//                 "                and c.TABLE_NAME  = cc.TABLE_NAME\n" +
//                 "                and c.COLUMN_NAME = cc.CONSTRAINT_NAME\n" +
//                 "        where 1 = 1\n" +
//                 "            and c.TABLE_SCHEMA = ?\n" +
//                 "            and c.TABLE_NAME   = ?\n" +
//                 "        order by c.ORDINAL_POSITION asc", schemaName, tableName);
//
//         return JSON.parseObject(JSONObject.toJSONString(queryResponse), new TypeReference<List<TableColumnInfo>>() {
//         });
//     }
//
//     /**
//      * 获取表的外键
//      *
//      * @param schemaName 表所有者
//      * @param tableName  表名
//      * @return
//      */
//     @Override
//     public List<TableForeignKeyInfo> getTableForeignKeyInfo(String schemaName, String tableName) {
//         List<LinkedHashMap<String, Object>> queryResponse = this.getJdbcTemplate().query("select\n" +
//                 "            substring(isf.ID, POSITION('/' in isf.REF_NAME) + 1) name,\n" +
//                 "            left(isf.FOR_NAME, POSITION('/' in isf.FOR_NAME) - 1) schemaName,\n" +
//                 "            left(isf.REF_NAME, POSITION('/' in isf.REF_NAME) - 1) referenceSchemaName,\n" +
//                 "            substring(isf.FOR_NAME, POSITION('/' in isf.FOR_NAME) + 1) tableName,\n" +
//                 "            substring(isf.REF_NAME, POSITION('/' in isf.REF_NAME) + 1) referenceTableName,\n" +
//                 "            isf.N_COLS columnCount,\n" +
//                 "            case\n" +
//                 "                when isf.TYPE in(0, 4, 8, 32) then 4\n" +
//                 "                when isf.TYPE in(1, 5, 9, 33) then 1\n" +
//                 "                when isf.TYPE in(2, 6, 10, 34) then 2\n" +
//                 "                when isf.TYPE in(16, 20, 24, 48) then 0 end deleteReferentialAction,\n" +
//                 "            case\n" +
//                 "                when isf.TYPE in(0, 1, 2, 16) then 4\n" +
//                 "                when isf.TYPE in(4, 5, 6, 20) then 1\n" +
//                 "                when isf.TYPE in(8, 9, 10, 24) then 2\n" +
//                 "                when isf.TYPE in(32, 33, 34, 48) then 0 end updateReferentialAction,\n" +
//                 "            case\n" +
//                 "                when isf.TYPE in(0, 4, 8, 32) then 'RESTRICT'\n" +
//                 "                when isf.TYPE in(1, 5, 9, 33) then 'CASCADE'\n" +
//                 "                when isf.TYPE in(2, 6, 10, 34) then 'SET_NULL'\n" +
//                 "                when isf.TYPE in(16, 20, 24, 48) then 'NO_ACTION' end deleteReferentialActionDesc,\n" +
//                 "            case\n" +
//                 "                when isf.TYPE in(0, 1, 2, 16) then 'RESTRICT'\n" +
//                 "                when isf.TYPE in(4, 5, 6, 20) then 'CASCADE'\n" +
//                 "                when isf.TYPE in(8, 9, 10, 24) then 'SET_NULL'\n" +
//                 "                when isf.TYPE in(32, 33, 34, 48) then 'NO_ACTION' end updateReferentialActionDesc,\n" +
//                 "            isfc.FOR_COL_NAME columnName,\n" +
//                 "            isfc.REF_COL_NAME referenceColumnName\n" +
//                 "        from information_schema.INNODB_SYS_FOREIGN isf\n" +
//                 "        left join information_schema.INNODB_SYS_FOREIGN_COLS isfc\n" +
//                 "            on isf.id = isfc.id\n" +
//                 "        where isf.FOR_NAME = ?", schemaName + "/" + tableName);
//
//         return JSON.parseObject(JSONObject.toJSONString(queryResponse), new TypeReference<List<TableForeignKeyInfo>>() {
//         });
//     }
//
//     @Override
//     public List<TableCheckConstraintInfo> getTableCheckConstraintInfo(String schemaName, String tableName) {
//
//         List<LinkedHashMap<String, Object>> queryResponse = this.getJdbcTemplate().query("select " +
//                 "t.CONSTRAINT_NAME, " +
//                 "t.CHECK_CLAUSE " +
//                 "from information_schema.CHECK_CONSTRAINTS t " +
//                 "where t.CONSTRAINT_SCHEMA=? and t.TABLE_NAME = ?", schemaName, tableName);
//
//         return JSON.parseObject(JSONObject.toJSONString(queryResponse), new TypeReference<List<TableCheckConstraintInfo>>() {
//         });
//     }
//
//     /**
//      * 统计表数据
//      *
//      * @param schemaName 表所有者
//      * @param tableName  表名
//      * @return
//      */
//     @Override
//     public long selectCount(String schemaName, String tableName) {
//
//         List<LinkedHashMap<String, Object>> queryResponse =
//                 this.getJdbcTemplate().query(String.format("select count(0) tableCount from %s.%s", schemaName, tableName));
//
//         return NumberUtils.toLong(queryResponse.get(0).get("tableCount"));
//     }
//
//     /**
//      * 查询数据
//      *
//      * @param schemaName 表所有者
//      * @param tableName  表名
//      * @param pageNum    页码
//      * @param pageSize   每页大小
//      * @return
//      */
//     @Override
//     public List<LinkedHashMap<String, Object>> selectList(String schemaName,
//                                                           String tableName,
//                                                           int pageNum,
//                                                           int pageSize) {
//         return this.getJdbcTemplate().query(String.format("select * from %s.%s", schemaName, tableName));
//     }
//
//
//     //////////////////////////////// SLAVE //////////////////////////////////
//
//     /**
//      * 获取表的基本信息
//      *
//      * @param schemaName 表所有者
//      * @param tableName  表名
//      * @return
//      */
//     public TableExtentInfo getTableExtentInfo(String schemaName, String tableName) {
//
//         List<LinkedHashMap<String, Object>> queryResponse = this.getJdbcTemplate().query("select\n" +
//                 "            t.TABLE_COLLATION defaultCollation,\n" +
//                 "            left(t.TABLE_COLLATION, position('_' in t.TABLE_COLLATION) - 1) defaultCharset\n" +
//                 "        from information_schema.TABLES t\n" +
//                 "        where t.TABLE_SCHEMA = ?\n" +
//                 "            and t.TABLE_NAME = ?\n" +
//                 "            and t.TABLE_TYPE = 'BASE TABLE'\n" +
//                 "            and t.TEMPORARY = 'N'\n" +
//                 "            limit 1", schemaName, tableName);
//
//         if (queryResponse.size() == 0) {
//             return null;
//         }
//
//         return JSONObject.toJavaObject((JSON) JSONObject.toJSON(queryResponse.get(0)), TableExtentInfo.class);
//     }
//
//
//
//     /**
//      * 删除表
//      *
//      * @param tableInfo 表信息
//      * @return
//      */
//     @Override
//     public void dropTable(TableInfo tableInfo) {
//         this.getJdbcTemplate().execute(String.format("drop table `%s`", tableInfo.getTableName()));
//     }
//
//     /**
//      * 创建表
//      *
//      * @param tableInfo 表信息
//      * @return
//      */
//     @Override
//     public void createTable(TableInfo tableInfo) {
//
//         StringBuilder ddlBuilder = new StringBuilder();
//         ddlBuilder.append("create table IF NOT EXISTS `").append(tableInfo.getTableName()).append("` ( ");
//         // 列
//         tableInfo.getTableColumnInfoList().forEach(tableColumnInfo -> {
//
//             ddlBuilder.append("`").append(tableColumnInfo.getColumnName()).append("` ").append(tableColumnInfo.getColumnType());
//             if (tableColumnInfo.isNotNull()) {
//                 ddlBuilder.append(" NOT NULL ");
//             }
//             if (StringUtils.isNotEmpty(tableColumnInfo.getColumnComment())) {
//                 ddlBuilder.append(" COMMENT '").append(tableColumnInfo.getColumnComment()).append("'");
//             }
//
//             if (tableInfo.getTableExtentInfo() == null ||
//                     (StringUtils.isNotEmpty(tableColumnInfo.getCollationName()) && tableInfo.getTableExtentInfo() != null && !tableColumnInfo.getCollationName().equals(tableInfo.getTableExtentInfo().getDefaultCollation()))) {
//                 ddlBuilder.append(" CHARACTER SET '").append(tableColumnInfo.getCharsetName()).append("'")
//                         .append(" COLLATE ").append(tableColumnInfo.getCollationName());
//             }
//             if (tableColumnInfo.getDefaultValue() != null) {
//                 ddlBuilder.append(" DEFAULT ").append(tableColumnInfo.getDefaultValue());
//             }
//             if (StringUtils.isNotEmpty(tableColumnInfo.getExtra())) {
//                 ddlBuilder.append(" ").append(tableColumnInfo.getExtra());
//             }
//             // if(StringUtils.isNotEmpty(tableColumnInfo.getCheckClause())){
//             //     ddlBuilder.append(" CHECK (").append(tableColumnInfo.getCheckClause()).append(")");
//             // }
//
//             ddlBuilder.append(", ");
//
//         });
//
//         // <!-- 索引、约束 -->
//         if (tableInfo.getTableIndexInfoMap().size() > 0) {
//             tableInfo.getTableIndexInfoMap().forEach((key, indexInfoList) -> {
//                 if (CollectionUtils.isEmpty(indexInfoList)) {
//                     return;
//                 }
//
//                 if (indexInfoList.get(0).isClusteredIndex()) {
//                     ddlBuilder.append(" PRIMARY KEY (");
//                     indexInfoList.forEach(indexInfo -> ddlBuilder.append("`").append(indexInfo.getColumnName()).append("`, "));
//                     ddlBuilder.delete(ddlBuilder.length() - 2, ddlBuilder.length());
//                     ddlBuilder.append("), ");
//                 }
//             });
//         }
//
//         //检查约束
//         // if(tableInfo.getTableCheckConstraintInfoList().size() > 0){
//         //     tableInfo.getTableCheckConstraintInfoList().forEach(checkConstraintInfo -> {
//         //         ddlBuilder.append(", CONSTRAINT `").append(checkConstraintInfo.getConstraintName()).append("` CHECK ( ")
//         //                 .append(checkConstraintInfo.getCheckClause()).append(" ), ");
//         //     });
//         //     ddlBuilder.delete(ddlBuilder.length() - 2, ddlBuilder.length());
//         // }
//
//         ddlBuilder.delete(ddlBuilder.length() - 2, ddlBuilder.length());
//
//         ddlBuilder.append(")");
//
//         // 表信息
//         if (StringUtils.isNotEmpty(tableInfo.getCurrentIncrementValue())) {
//             ddlBuilder.append(" AUTO_INCREMENT=").append(tableInfo.getCurrentIncrementValue());
//         }
//
//         if(tableInfo.getTableExtentInfo() != null){
//             ddlBuilder.append(" DEFAULT COLLATE=").append(tableInfo.getTableExtentInfo().getDefaultCollation());
//         }
//
//         if (StringUtils.isNotEmpty(tableInfo.getTableComment())) {
//             ddlBuilder.append(" COMMENT='").append(tableInfo.getTableComment()).append("'");
//         }
//
//         this.getJdbcTemplate().execute(ddlBuilder.toString());
//     }
//
//     /**
//      * 查询表的数量
//      *
//      * @param schemaName 表的所有者
//      * @param tableName  表名
//      * @return
//      */
//     @Override
//     public int countTable(String schemaName, String tableName) {
//         List<LinkedHashMap<String, Object>> queryResponse = this.getJdbcTemplate().query("" +
//                 "select count(0) tableCount from information_schema.TABLES t where t.TABLE_SCHEMA = ? and t.TABLE_NAME = ?", schemaName, tableName);
//
//         return NumberUtils.toInt(queryResponse.get(0).get("tableCount"));
//     }
//
//
// }
