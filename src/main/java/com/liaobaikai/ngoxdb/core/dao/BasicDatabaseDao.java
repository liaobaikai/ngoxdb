package com.liaobaikai.ngoxdb.core.dao;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.liaobaikai.ngoxdb.bean.NgoxDbMaster;
import com.liaobaikai.ngoxdb.bean.PreparedPagination;
import com.liaobaikai.ngoxdb.bean.info.ColumnInfo;
import com.liaobaikai.ngoxdb.bean.info.DatabaseInfo;
import com.liaobaikai.ngoxdb.bean.info.IndexInfo2;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.bean.rs.*;
import com.liaobaikai.ngoxdb.boot.JdbcTemplate;
import com.liaobaikai.ngoxdb.core.constant.JdbcDataType;
import com.liaobaikai.ngoxdb.core.dialect.DatabaseDialect;
import com.liaobaikai.ngoxdb.core.entity.NgoxDbRelayLog;
import com.liaobaikai.ngoxdb.utils.CommonUtils;
import com.liaobaikai.ngoxdb.utils.NumberUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.lang.NonNull;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 基础数据库访问
 *
 * @author baikai.liao
 * @Time 2021-01-27 15:14:14
 */
public abstract class BasicDatabaseDao implements DatabaseDao {

    private final NgoxDbMaster ngoxDbMaster;
    private final int maxIdentifierLength;

    public BasicDatabaseDao(@NonNull NgoxDbMaster ngoxDbMaster) {
        this.ngoxDbMaster = ngoxDbMaster;
        Integer maxLength = getJdbcTemplate().execute((ConnectionCallback<Integer>) con -> con.getMetaData().getMaxColumnNameLength());
        this.maxIdentifierLength = maxLength == null ? 0 : maxLength;
    }

    @Override
    public DatabaseDialect getDatabaseDialect() {
        return this.ngoxDbMaster.getDatabaseDialect();
    }

    @Override
    public NgoxDbMaster getNgoxDbMaster() {
        return this.ngoxDbMaster;
    }

    /**
     * 初始化数据库信息
     */
    protected abstract DatabaseInfo initDatabaseInfo();

    @Override
    public int getMaxIdentifierLength() {
        return this.maxIdentifierLength;
    }

    @Override
    public JdbcTemplate getJdbcTemplate() {
        return this.ngoxDbMaster.getJdbcTemplate();
    }

    private String postName(String name) {
        if (name != null) {
            if (getDatabaseDialect().defaultUpperCaseName()) {
                return name.toUpperCase();
            } else if (getDatabaseDialect().defaultLowerCaseName()) {
                return name.toLowerCase();
            }
        }
        return name;
    }

    /**
     * 获取所有的表
     *
     * @param tableName 需要查询的表名
     * @return 查询到的表信息
     */
    @Override
    public List<TableInfo> getTables(String... tableName) {

        return this.getJdbcTemplate().execute((ConnectionCallback<List<TableInfo>>) con -> {
            DatabaseMetaData metaData = con.getMetaData();
            List<TableInfo> tableList = new ArrayList<>();

            if (tableName.length == 0) {
                getTable(con, metaData, tableList, "%");
            } else {
                for (String name : tableName) {
                    getTable(con, metaData, tableList, postName(name));
                }
            }

            if (tableList.size() == 0) {
                getLogger().info("Query table name: {}, total: 0",
                        JSONObject.toJSONString(tableName));
            } else {
                StringBuilder stringBuilder = new StringBuilder();
                for (TableInfo tableInfo : tableList) {
                    stringBuilder.append(tableInfo.getTableName()).append(", ");
                }
                stringBuilder.delete(stringBuilder.length() - 2, stringBuilder.length());
                getLogger().info("Query table name: {}, Total: {}, Table list: {}",
                        JSONObject.toJSONString(tableName),
                        tableList.size(),
                        stringBuilder.toString());
            }

            return tableList;
        });
    }

    private void getTable(Connection con, DatabaseMetaData metaData, List<TableInfo> tableList, String table) throws SQLException {
        ResultSet rs = metaData.getTables(con.getCatalog(), con.getSchema(), table, new String[]{"TABLE"});
        while (rs.next()) {
            tableList.add(JSON.toJavaObject(getJsonObject(rs), TableInfo.class));
        }
    }

    @Override
    public List<ColumnInfo> getColumns(String... tableName) {

        return this.getJdbcTemplate().execute((ConnectionCallback<List<ColumnInfo>>) con -> {
            DatabaseMetaData metaData = con.getMetaData();
            List<ColumnInfo> columnList = new ArrayList<>();

            if (tableName.length == 0) {
                getColumn(con, metaData, columnList, "%");
            } else {
                for (String name : tableName) {
                    getColumn(con, metaData, columnList, postName(name));
                }
            }

            return columnList;
        });
    }

    private void getColumn(Connection con, DatabaseMetaData metaData, List<ColumnInfo> columnList, String name) throws SQLException {
        try (ResultSet rs = metaData.getColumns(con.getCatalog(), con.getSchema(), name, "%")) {
            while (rs.next()) {
                columnList.add(JSON.toJavaObject(getJsonObject(rs), ColumnInfo.class));
            }
        }
    }

    private JSONObject getJsonObject(ResultSet rs) throws SQLException {
        JSONObject jsonObject = new JSONObject();
        final int columnCount = rs.getMetaData().getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            jsonObject.put(JdbcUtils.lookupColumnName(rs.getMetaData(), i + 1), rs.getObject(i + 1));
        }
        return jsonObject;
    }


    @Override
    public List<PrimaryKey> getPrimaryKeys(String tableName) {
        return this.getJdbcTemplate().execute((ConnectionCallback<List<PrimaryKey>>) conn -> {
            DatabaseMetaData metaData = conn.getMetaData();
            List<PrimaryKey> resultList = new ArrayList<>();

            try (ResultSet rs = metaData.getPrimaryKeys(conn.getCatalog(), conn.getSchema(), postName(tableName))) {
                while (rs.next()) {
                    resultList.add(JSON.toJavaObject(getJsonObject(rs), PrimaryKey.class));
                }
            }

            return resultList;
        });
    }

    @Override
    public List<ImportedKey> getImportedKeys(String tableName) {

        return this.getJdbcTemplate().execute((ConnectionCallback<List<ImportedKey>>) conn -> {
            DatabaseMetaData metaData = conn.getMetaData();
            List<ImportedKey> resultList = new ArrayList<>();

            try (ResultSet rs = metaData.getImportedKeys(conn.getCatalog(), conn.getSchema(), postName(tableName))) {
                while (rs.next()) {
                    resultList.add(JSON.toJavaObject(getJsonObject(rs), ImportedKey.class));
                }
            }

            return resultList;
        });
    }

    @Override
    public List<ExportedKey> getExportedKeys(String tableName) {

        return this.getJdbcTemplate().execute((ConnectionCallback<List<ExportedKey>>) con -> {
            DatabaseMetaData metaData = con.getMetaData();
            List<ExportedKey> resultList = new ArrayList<>();

            try (ResultSet rs = metaData.getExportedKeys(con.getCatalog(), con.getSchema(), postName(tableName))) {
                while (rs.next()) {
                    resultList.add(JSON.toJavaObject(getJsonObject(rs), ExportedKey.class));
                }
            }

            return resultList;
        });
    }


    @Override
    public List<VersionColumn> getVersionColumns(String tableName) {

        return this.getJdbcTemplate().execute((ConnectionCallback<List<VersionColumn>>) con -> {
            DatabaseMetaData metaData = con.getMetaData();
            List<VersionColumn> resultList = new ArrayList<>();

            try (ResultSet rs = metaData.getVersionColumns(con.getCatalog(), con.getSchema(), postName(tableName))) {
                while (rs.next()) {
                    resultList.add(JSON.toJavaObject(getJsonObject(rs), VersionColumn.class));
                }
            }


            return resultList;
        });
    }

    @Override
    public List<Attribute> getAttributes(String... typeName) {

        return this.getJdbcTemplate().execute((ConnectionCallback<List<Attribute>>) con -> {
            DatabaseMetaData metaData = con.getMetaData();
            List<Attribute> resultList = new ArrayList<>();

            if (typeName.length == 0) {
                getType(con, metaData, resultList, "%");
            } else {
                for (String name : typeName) {
                    getType(con, metaData, resultList, postName(name));
                }
            }

            return resultList;
        });
    }

    private void getType(Connection con, DatabaseMetaData metaData, List<Attribute> resultList, String name) throws SQLException {
        try (ResultSet rs = metaData.getAttributes(con.getCatalog(), con.getSchema(), name, "%")) {
            while (rs.next()) {
                resultList.add(JSON.toJavaObject(getJsonObject(rs), Attribute.class));
            }
        }
    }

    @Override
    public List<IndexInfo2> getIndexInfo(String tableName) {

        List<IndexInfo2> indexInfoList = this.getJdbcTemplate().execute((ConnectionCallback<List<IndexInfo2>>) con -> {
            DatabaseMetaData metaData = con.getMetaData();
            List<IndexInfo2> resultList = new ArrayList<>();

            try (ResultSet rs = metaData.getIndexInfo(con.getCatalog(), con.getSchema(), postName(tableName), false, false)) {
                while (rs.next()) {
                    resultList.add(JSON.toJavaObject(getJsonObject(rs), IndexInfo2.class));
                }
            }

            return resultList;
        });
        removePrimaryIndex(indexInfoList);
        return indexInfoList;
    }

    @Override
    public void removePrimaryIndex(List<IndexInfo2> indexInfoList) {
        if (indexInfoList == null) {
            return;
        }
        // type = 1
        // 主键默认就是索引，因此需要删除主键的索引信息。在创建表的同时不需要再次创建一个唯一的索引。
        indexInfoList.removeIf(ii -> ii.getType() == DatabaseMetaData.tableIndexClustered);
    }

    /**
     * 获取表的数量
     *
     * @param tableName 表名
     * @return 数量
     */
    @Override
    public int getTableCount(String tableName) {

        Integer tableCount = this.getJdbcTemplate().execute((ConnectionCallback<Integer>) conn -> {
            DatabaseMetaData metaData = conn.getMetaData();
            int count = 0;
            try (ResultSet rs = metaData.getTables(conn.getCatalog(), conn.getSchema(), null, new String[]{"TABLE"})) {
                while (rs.next()) {
                    if (rs.getString("TABLE_NAME").equalsIgnoreCase(tableName)) {
                        count++;
                        break;
                    }
                }
            }

            return count;
        });

        // Unnecessary unboxing....
        return tableCount == null ? 0 : tableCount;
    }

    @Override
    public boolean existsTable(String tableName) {
        return this.getTableCount(tableName) > 0;
    }

    /**
     * 获取表的行的数量
     *
     * @param tableName 表名
     * @return 数量
     */
    @Override
    public long getTableRowCount(String tableName) {
        List<Map<String, Object>> queryResponse =
                this.getJdbcTemplate().queryForList(String.format("select count(0) from %s", this.getDatabaseDialect().toLookupName(tableName)));
        Map<String, Object> map = queryResponse.get(0);
        Object value = CommonUtils.getFirstValue(map);
        return NumberUtils.toLong(value == null ? 0 : value);
    }

    @Override
    public void dropTable(String tableName) {
        if (this.existsTable(tableName)) {
            String stmt = this.getDatabaseDialect().getDropTableString(tableName);
            this.execute(stmt);
            getLogger().info("[{}] {}", this.getNgoxDbMaster().getDatabaseConfig().getName(), stmt);
        }
    }

    @Override
    public void dropForeignKey(String tableName) {
        // 获取是否存在外键
        List<ImportedKey> importedKeys = this.getImportedKeys(tableName);

        // 删除表的外键约束语句
        for (ImportedKey importedKey : importedKeys) {
            String stmt = String.format("alter table %s %s %s",
                    this.getDatabaseDialect().toLookupName(importedKey.getFkTableName()),
                    this.getDatabaseDialect().getDropForeignKeyString(),
                    this.getDatabaseDialect().toLookupName(importedKey.getFkName()));
            getLogger().info("Table {} exists foreign key, {}", this.getDatabaseDialect().toLookupName(tableName), stmt);
            this.execute(stmt);
        }
    }

    @Override
    public void deleteTable(String tableName) {
        this.execute("delete from " + this.getDatabaseDialect().toLookupName(tableName));
    }

    /**
     * 截断表
     *
     * @param tableName 表名
     */
    @Override
    public void truncateTable(String tableName) {
        if (!this.getDatabaseDialect().supportsTruncateTable()) {
            return;
        }
        this.execute("truncate table " + this.getDatabaseDialect().toLookupName(tableName));
    }

    /**
     * 创建表
     */
    @Override
    public void createLogTable() {
        String sqlBuilder = String.format("create table %s ( " +
                        "log_id varchar(%s) unique," +
                        "log_table_name varchar(128)," +
                        "log_type varchar(32)," +
                        "log_text %s," +
                        "log_used int)",
                postName(NgoxDbRelayLog.TABLE_NAME),
                UUID.randomUUID().toString().length(),
                this.getDatabaseDialect().getDataTypeMap().get(JdbcDataType.LONGVARCHAR));

        this.getJdbcTemplate().execute(sqlBuilder);
    }

    /**
     * 更新元数据
     *
     * @param entity 实体
     */
    @Override
    public void updateLogRows(NgoxDbRelayLog entity) {
        String sqlBuilder = String.format("update %s set log_used = ? where log_id = ?", postName(NgoxDbRelayLog.TABLE_NAME));
        this.getJdbcTemplate().update(sqlBuilder, entity.getLogUsed(), entity.getLogId());
    }

    /**
     * 插入源数据
     *
     * @param entity 实体
     */
    @Override
    public void insertLogRows(NgoxDbRelayLog entity) {
        String sqlBuilder = String.format("INSERT INTO %s (log_id, log_table_name, log_type, log_text, log_used) VALUES (?, ?, ?, ?, ?)", postName(NgoxDbRelayLog.TABLE_NAME));
        this.getJdbcTemplate().update(sqlBuilder,
                entity.getLogId(), entity.getLogTableName(), entity.getLogType(), entity.getLogText(), entity.getLogUsed());
    }

    /**
     * 删除源数据
     *
     * @param tableName 表名
     */
    @Override
    public void deleteLogRows(String tableName) {
        String sqlBuilder = String.format("delete from %s where log_table_name = ?", postName(NgoxDbRelayLog.TABLE_NAME));
        this.getJdbcTemplate().update(sqlBuilder, tableName);
    }

    @Override
    public void dropLogTable() {
        this.getJdbcTemplate().execute(getDatabaseDialect().getDropTableString(postName(NgoxDbRelayLog.TABLE_NAME)));
    }

    @Override
    public List<NgoxDbRelayLog> getLogRows(Integer isUsed, String... tableNames) {

        StringBuilder sBuilder = new StringBuilder();
        sBuilder.append("select * from ").append(postName(NgoxDbRelayLog.TABLE_NAME)).append(" where ");

        Object[] params = new Object[tableNames.length + 1];
        int i = 0;
        if (tableNames.length > 0) {
            sBuilder.append("log_table_name in ( ");

            for (String tableName : tableNames) {
                sBuilder.append("?,");
                tableNames[i] = tableName;
                i++;
            }
            sBuilder.delete(sBuilder.length() - 1, sBuilder.length()).append(") and");
        }
        params[i] = isUsed;

        sBuilder.append(" log_used = ?");

        return this.getJdbcTemplate().queryForList(sBuilder.toString(), NgoxDbRelayLog.class, params);
    }

    /**
     * 获取参数
     *
     * @param sqlBuilder  SQL
     * @param tableName   表名
     * @param placeHolder 占位符
     * @param param0      需要传入的第一个参数
     * @return 参数数组
     */
    public Object[] getParams(StringBuilder sqlBuilder, String[] tableName, String placeHolder, String param0) {

        Object[] params;
        String[] ss = new String[tableName.length];

        if (param0 != null) {
            params = new Object[tableName.length + 1];
            params[0] = param0;
        } else {
            params = new Object[tableName.length];
            if (tableName.length == 0) {
                return params;
            }
        }

        if (placeHolder == null) {
            placeHolder = "?";
        }

        for (int i = 0; i < tableName.length; i++) {
            ss[i] = placeHolder;

            if (param0 != null) {
                params[i + 1] = tableName[i];
            } else {
                params[i] = tableName[i];
            }

        }

        if (tableName.length > 0) {
            sqlBuilder.replace(sqlBuilder.length() - 2, sqlBuilder.length() - 1, String.join(", ", ss));
        }

        return params;
    }

    /**
     * 获取最终的默认值效果，如数字，返回就是数字，字符串需要加上 '' ，函数默认值无需添加。
     *
     * @param defaultValue 默认值
     * @param jdbcType     数据类型 {@link java.sql.Types}
     * @return
     */
    protected String getFinalDefaultValue(String defaultValue, int jdbcType) {

        // Number
        if (ArrayUtils.contains(NUMBER_JDBC_TYPES, jdbcType)) {
            return defaultValue;
        } else if (ArrayUtils.contains(STRING_JDBC_TYPES, jdbcType)) {
            // random("abcdefghih")
            return "'" + defaultValue + "'";
        } else if (ArrayUtils.contains(DATE_TIME_JDBC_TYPES, jdbcType)) {
            String newValue = defaultValue.replaceAll("[-/:;.| ]*", "");
            if (org.apache.commons.lang3.math.NumberUtils.isDigits(newValue)) {
                return "'" + defaultValue + "'";
            }
        }

        return defaultValue;
    }

    @Override
    public List<Object[]> pagination(String tableName,
                                     String[] queryColumnNames,
                                     String[] orderColumnNames,
                                     boolean isPrimaryKeyOrder,
                                     int offset,
                                     int limit) {

        PreparedPagination preparedPagination =
                this.getDatabaseDialect().getPreparedPagination(tableName, queryColumnNames, orderColumnNames, isPrimaryKeyOrder, offset, limit);

        // this.getLogger().info("[{}] {}; limit={}, offset={}", this.getJdbcTemplate().getDatabaseConfig().getName(), preparedPagination.getPreparedSql(),
        //         limit, offset);

        return this.query(preparedPagination.getPreparedSql(), preparedPagination.getParamValues());
    }

    @Override
    public List<Object[]> query(String sql, Object[] paramValues) {
        // 查询某一页的所有的数据
        // return this.getJdbcTemplate().execute((ConnectionCallback<List<Object[]>>) con -> {
        //     List<Object[]> rowArgs = new ArrayList<>();
        //     try(PreparedStatement ps = con.prepareStatement(sql)) {
        //
        //         for(int x = 0, len = paramValues.length; x < len; x++){
        //             ps.setObject(x + 1, paramValues[x]);
        //         }
        //
        //         try(ResultSet rs = ps.executeQuery()) {
        //             while (rs.next()){
        //                 ResultSetMetaData metaData = rs.getMetaData();
        //                 Object[] values = new Object[metaData.getColumnCount()];
        //                 for (int i = 0, len = values.length; i < len; i++) {
        //                     // 源数据库处理为通用类型
        //                     Object value = rs.getObject(i + 1);
        //                     if (value instanceof Clob) {
        //                         // clob & nclob
        //                         try {
        //                             values[i] = StringUtils.clob2String((Clob) value);
        //                         } catch (SQLException e) {
        //                             e.printStackTrace();
        //                         }
        //                         continue;
        //                     }
        //                     // 转换为通用的类型
        //                     values[i] = getDatabaseDialect().getSqlGenericType(value);
        //                 }
        //                 rowArgs.add(values);
        //             }
        //         }
        //     }
        //     return rowArgs;
        // });

        // this.getLogger().info("sql: {}, paramValues: {}", sql, JSONObject.toJSONString(paramValues));

        // return this.getJdbcTemplate().execute(sql, (PreparedStatementCallback<List<Object[]>>) ps -> {
        //     List<Object[]> rsp = new ArrayList<>();
        //     try (Connection con = ps.getConnection()){
        //         ps
        //         ps.execute();
        //     }
        //     return rsp;
        // });

        return this.getJdbcTemplate().query(sql, paramValues, (rs, rowNum) -> {
            Object[] values = new Object[rs.getMetaData().getColumnCount()];
            for (int i = 0, len = values.length; i < len; i++) {
                // 源数据库处理为通用类型
                // Object value = rs.getObject(i + 1);
                // if (value instanceof Clob) {
                //     // clob & nclob
                //     try {
                //         values[i] = StringUtils.clob2String((Clob) value);
                //     } catch (SQLException e) {
                //         e.printStackTrace();
                //     }
                //     continue;
                // }
                // 转换为通用的类型
                // values[i] = this.getDatabaseDialect().getSqlGenericType(value);
                values[i] = rs.getObject(i + 1);
            }
            return values;
        });
    }

    @Override
    public List<Object[]> query(String sql, Map<String, Object[]> condition) {
        return null;
    }

    @Override
    public void execute(String stmt) {
        this.getJdbcTemplate().execute(stmt);
    }

    @Override
    public void batchUpdate(String stmt, List<Object[]> batchArgs) {
        this.getJdbcTemplate().batchUpdate(stmt, batchArgs);
    }
}
