// package com.liaobaikai.ngoxdb.dao;
//
// import com.alibaba.fastjson.JSON;
// import com.alibaba.fastjson.JSONObject;
// import com.alibaba.fastjson.TypeReference;
// import com.liaobaikai.ngoxdb.info.*;
// import com.liaobaikai.ngoxdb.types.SQLServerType;
// import com.liaobaikai.ngoxdb.utils.NumberUtils;
// import com.liaobaikai.ngoxdb.utils.SQLServerUtils;
// import com.liaobaikai.ngoxdb.jdbctemplate.SimpleJdbcTemplate;
// import org.springframework.jdbc.core.JdbcTemplate;
// import org.springframework.util.CollectionUtils;
//
// import java.util.LinkedHashMap;
// import java.util.List;
//
// /**
//  * @author baikai.liao
//  * @Time 2021-01-23 01:11:10
//  */
// public class SQLServerDao extends BasicDatabaseDao {
//
//     public SQLServerDao(SimpleJdbcTemplate jdbcTemplate) {
//         super(jdbcTemplate);
//     }
//
//     @Override
//     public List<TableInfo> getTableInfo(String schemaName) {
//
//         List<LinkedHashMap<String, Object>> queryResponse = this.getJdbcTemplate().query("select\n" +
//                 "            s.name tableSchema,\n" +
//                 "            t.name tableName,\n" +
//                 "            ep.value tableComment\n" +
//                 "        from sys.tables t\n" +
//                 "        left join sys.schemas s\n" +
//                 "            on t.schema_id = s.schema_id\n" +
//                 "        left join sys.extended_properties ep\n" +
//                 "            on t.object_id = ep.major_id and ep.minor_id = 0\n" +
//                 "        where s.name = ?", schemaName);
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
//                 "            s.name tableSchema,\n" +
//                 "            t.name tableName,\n" +
//                 "            ep.value tableComment\n" +
//                 "        from sys.tables t\n" +
//                 "        left join sys.schemas s\n" +
//                 "            on t.schema_id = s.schema_id\n" +
//                 "        left join sys.extended_properties ep\n" +
//                 "            on t.object_id = ep.major_id and ep.minor_id = 0\n" +
//                 "        where s.name = ? and t.name = ?", schemaName, tableName);
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
//                 "            s.name schemaName,\n" +
//                 "            o.name tableName,\n" +
//                 "            i.index_id,\n" +
//                 "            c.name columnName,\n" +
//                 "            i.name indexName,\n" +
//                 "            case when i.is_unique = 0 and i.is_primary_key = 0 then 1 else 0 end isNonUniqueSecondaryIndex,\n" +
//                 "            case when i.is_unique = 1 and i.is_primary_key = 0 then 1 else 0 end isUniqueNonClusteredIndex,\n" +
//                 "            i.is_primary_key isClusteredIndex,\n" +
//                 "            ik.keyno columnPosition,\n" +
//                 "            -1 columnCount\n" +
//                 "        from sysindexkeys ik\n" +
//                 "        left join syscolumns c\n" +
//                 "            on ik.id = c.id and ik.colid = c.colid\n" +
//                 "        left join sys.indexes i\n" +
//                 "            on i.object_id = ik.id and i.index_id = ik.indid\n" +
//                 "        left join sys.objects o\n" +
//                 "            on o.object_id = i.object_id\n" +
//                 "        left join sys.schemas s\n" +
//                 "            on o.schema_id = s.schema_id\n" +
//                 "        where s.name = ? and o.name = ? order by index_id asc", schemaName, tableName);
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
//                 "            s.name tableSchema,\n" +
//                 "            t.name tableName,\n" +
//                 "            c.name columnName,\n" +
//                 "            ep.value COLUMN_COMMENT,\n" +
//                 "            sc.text defaultValue,\n" +
//                 "            case c.isnullable when 1 then 0 else 1 end isNotNull,\n" +
//                 "            case when exists (\n" +
//                 "                SELECT 1 FROM sysobjects where xtype='PK' and parent_obj=c.id and name in (\n" +
//                 "                    SELECT name FROM sysindexes WHERE indid in(\n" +
//                 "                        SELECT indid FROM sysindexkeys WHERE id = c.id AND colid=c.colid\n" +
//                 "                    )\n" +
//                 "                )\n" +
//                 "            ) then 1 else 0 end isPrimaryKey,\n" +
//                 "            case when exists (\n" +
//                 "                SELECT 1 FROM sysobjects where xtype='UQ' and parent_obj=c.id and name in (\n" +
//                 "                    SELECT name FROM sysindexes WHERE indid in (\n" +
//                 "                        SELECT indid FROM sysindexkeys WHERE id = c.id AND colid=c.colid\n" +
//                 "                    )\n" +
//                 "                )\n" +
//                 "            ) then 1 else 0 end isUniqueKey,\n" +
//                 "            c.prec precision,\n" +
//                 "            c.scale scale,\n" +
//                 "            c.collation COLLATION_NAME,\n" +
//                 "            SERVERPROPERTY('SqlCharSetName') charsetName,\n" +
//                 "            st.name DATA_TYPE\n" +
//                 "        from syscolumns c\n" +
//                 "        left join sys.tables t\n" +
//                 "            on t.object_id = c.id\n" +
//                 "        left join systypes st\n" +
//                 "            on c.xusertype = st.xusertype\n" +
//                 "        left join sys.schemas s\n" +
//                 "            on t.schema_id = s.schema_id\n" +
//                 "        left join syscomments sc\n" +
//                 "            on c.cdefault = sc.id\n" +
//                 "        left join sys.extended_properties ep\n" +
//                 "            on t.object_id = ep.major_id and c.colid = ep.minor_id\n" +
//                 "        where s.name = ? and t.name = ? order by colorder asc", schemaName, tableName);
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
//
//         List<LinkedHashMap<String, Object>> queryResponse = this.getJdbcTemplate().query("select\n" +
//                 "            fk.name,\n" +
//                 "            s1.name schemaName,\n" +
//                 "            o1.name tableName,\n" +
//                 "            s2.name referenceSchemaName,\n" +
//                 "            o2.name referenceTableName,\n" +
//                 "            fkc.constraint_column_id columnCount,\n" +
//                 "            fk.delete_referential_action,\n" +
//                 "            fk.delete_referential_action_desc,\n" +
//                 "            fk.update_referential_action,\n" +
//                 "            fk.update_referential_action_desc,\n" +
//                 "            c1.name columnName,\n" +
//                 "            c2.name referenceColumnName,\n" +
//                 "            fk.is_system_named isAutoGen\n" +
//                 "        from sys.foreign_keys fk\n" +
//                 "        left join sys.objects o1\n" +
//                 "            on fk.parent_object_id = o1.object_id\n" +
//                 "        left join sys.objects o2\n" +
//                 "            on fk.referenced_object_id = o2.object_id\n" +
//                 "        left join sys.schemas s1\n" +
//                 "            on s1.schema_id = o1.schema_id\n" +
//                 "        left join sys.schemas s2\n" +
//                 "            on s2.schema_id = o2.schema_id\n" +
//                 "        left join sys.foreign_key_columns fkc\n" +
//                 "            on fk.object_id = fkc.constraint_object_id\n" +
//                 "        left join sys.columns c1\n" +
//                 "            on c1.object_id = fk.parent_object_id and c1.column_id = fkc.parent_column_id\n" +
//                 "        left join sys.columns c2\n" +
//                 "            on c2.object_id = fk.referenced_object_id and c2.column_id = fkc.referenced_column_id\n" +
//                 "        where s1.name = ? and o1.name = ?", schemaName, tableName);
//
//         return JSON.parseObject(JSONObject.toJSONString(queryResponse), new TypeReference<List<TableForeignKeyInfo>>() {
//         });
//     }
//
//     @Override
//     public List<TableCheckConstraintInfo> getTableCheckConstraintInfo(String schemaName, String tableName) {
//
//         List<LinkedHashMap<String, Object>> queryResponse = this.getJdbcTemplate().query("select " +
//                 "cc.name CONSTRAINT_NAME, " +
//                 "cc.definition CHECK_CLAUSE " +
//                 "from sys.check_constraints cc " +
//                 "where schema_id=SCHEMA_ID(?) and parent_object_id = OBJECT_ID(?)", schemaName, tableName);
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
//         List<LinkedHashMap<String, Object>> queryResponse =
//                 this.getJdbcTemplate().query(String.format("select count(0) count from %s.%s", schemaName, tableName));
//         return NumberUtils.toLong(queryResponse.get(0).get("count"));
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
//
//         return this.getJdbcTemplate().query(String.format("select * from %s.%s", schemaName, tableName));
//     }
//
//
//     //////////////////////////////// SLAVE //////////////////////////////////
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
//         try {
//             this.getJdbcTemplate().execute(String.format("drop table %s", tableInfo.getTableName()));
//         } catch (Exception e) {
//
//         }
//
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
//         // 判断数据库厂家是否相同
//         // 参数或变量: 不能对数据类型 int 指定列宽。
//
//         StringBuilder ddlBuilder = new StringBuilder();
//         ddlBuilder.append("create table ").append(tableInfo.getTableName()).append(" ( ");
//
//         // 列
//         tableInfo.getTableColumnInfoList().forEach(tableColumnInfo -> {
//
//             // 获取SQLServerType
//             SQLServerType sqlServerType = SQLServerType.getByJdbcType(tableColumnInfo.getJdbcType());
//
//             // 类型设置
//             // 没有精度不用增加
//             SQLServerUtils.buildColumnType(sqlServerType, tableColumnInfo);
//
//             ddlBuilder.append(tableColumnInfo.getColumnName()).append(" ").append(tableColumnInfo.getColumnType());
//             if (tableColumnInfo.isNotNull()) {
//                 ddlBuilder.append(" NOT NULL ");
//             }
//             if (tableColumnInfo.getDefaultValue() != null) {
//                 ddlBuilder.append(" DEFAULT ").append(tableColumnInfo.getDefaultValue());
//             }
//
//             ddlBuilder.append(", ");
//
//         });
//
//         // <!-- 索引、约束 -->
//         if (tableInfo.getTableIndexInfoMap().size() > 0) {
//
//             tableInfo.getTableIndexInfoMap().forEach((key, indexInfoList) -> {
//                 if (CollectionUtils.isEmpty(indexInfoList)) {
//                     return;
//                 }
//
//                 if (indexInfoList.get(0).isClusteredIndex()) {
//                     ddlBuilder.append(" PRIMARY KEY (");
//                     indexInfoList.forEach(indexInfo -> ddlBuilder.append(indexInfo.getColumnName()).append(", "));
//                     ddlBuilder.delete(ddlBuilder.length() - 2, ddlBuilder.length());
//                     ddlBuilder.append("), ");
//                 }
//             });
//         }
//
//         // 检查约束
//         // if(tableInfo.getTableCheckConstraintInfoList().size() > 0){
//         //     tableInfo.getTableCheckConstraintInfoList().forEach(checkConstraintInfo -> {
//         //         ddlBuilder.append(" CONSTRAINT ").append(checkConstraintInfo.getConstraintName()).append(" CHECK ( ")
//         //                 .append(checkConstraintInfo.getCheckClause()).append(" ), ");
//         //     });
//         // }
//
//         ddlBuilder.delete(ddlBuilder.length() - 2, ddlBuilder.length());
//
//         ddlBuilder.append(")");
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
//         List<LinkedHashMap<String, Object>> queryResponse =
//                 this.getJdbcTemplate().query("select COUNT(0) tableCount from sys.tables t where t.name = ?", tableName);
//         return NumberUtils.toInt(queryResponse.get(0).get("tableCount"));
//     }
// }
