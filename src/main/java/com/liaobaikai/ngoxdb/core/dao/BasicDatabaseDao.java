package com.liaobaikai.ngoxdb.core.dao;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.liaobaikai.ngoxdb.bean.info.ColumnInfo;
import com.liaobaikai.ngoxdb.bean.info.DatabaseInfo;
import com.liaobaikai.ngoxdb.bean.info.IndexInfo2;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.bean.rs.*;
import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
import com.liaobaikai.ngoxdb.core.entity.SlaveMetaDataEntity;
import com.liaobaikai.ngoxdb.utils.NumberUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

/**
 * 基础数据库访问
 *
 * @author baikai.liao
 * @Time 2021-01-27 15:14:14
 */
public abstract class BasicDatabaseDao implements DatabaseDao {

    private final JdbcTemplate2 jdbcTemplate;
    private final String catalog;
    private int maxColumnNameLength = 0;
    private int maxStatementLength = 0;
    private boolean doesMaxRowSizeIncludeBlobs;

    public BasicDatabaseDao(JdbcTemplate2 jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.catalog = jdbcTemplate.execute(new ConnectionCallback<String>() {
            @Nullable
            @Override
            public String doInConnection(@NonNull Connection con) throws SQLException, DataAccessException {
                DatabaseMetaData metaData = con.getMetaData();
                maxColumnNameLength = metaData.getMaxColumnNameLength();
                maxStatementLength = metaData.getMaxStatementLength();
                doesMaxRowSizeIncludeBlobs = metaData.doesMaxRowSizeIncludeBlobs();
                return con.getCatalog();
            }
        });
    }

    @Override
    public String getCatalog() {
        return this.catalog;
    }

    @Override
    public JdbcTemplate2 getJdbcTemplate() {
        return this.jdbcTemplate;
    }

    /**
     * 初始化数据库信息
     */
    protected abstract DatabaseInfo initDatabaseInfo();

    @Override
    public int getMaxColumnNameLength() {
        return this.maxColumnNameLength;
    }

    @Override
    public int getMaxStatementLength() {
        return this.maxStatementLength;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() {
        return this.doesMaxRowSizeIncludeBlobs;
    }

    /**
     * 获取所有的表
     *
     * @param tableName 需要查询的表名
     * @return 查询到的表信息
     */
    @Override
    public List<TableInfo> getTables(String... tableName) {

        getLogger().info("正在读取表信息...");

        return this.jdbcTemplate.execute((ConnectionCallback<List<TableInfo>>) conn -> {
            DatabaseMetaData metaData = conn.getMetaData();
            List<TableInfo> tableList = new ArrayList<>();

            if (tableName.length == 0) {
                getTable(metaData, tableList, "%");
            } else {
                for (String name : tableName) {
                    getTable(metaData, tableList, name);
                }
            }

            if (tableList.size() == 0) {
                getLogger().info("Master, Query table name: {}, total: 0", JSONObject.toJSONString(tableName));
            } else {
                StringBuilder stringBuilder = new StringBuilder();
                for (TableInfo tableInfo : tableList) {
                    stringBuilder.append(tableInfo.getTableName()).append(", ");
                }
                stringBuilder.delete(stringBuilder.length() - 2, stringBuilder.length());
                getLogger().info("Query table name: {}, Total: {}, Table list: {}", JSONObject.toJSONString(tableName), tableList.size(), stringBuilder.toString());
            }

            return tableList;
        });
    }

    private void getTable(DatabaseMetaData metaData, List<TableInfo> tableList, String table) throws SQLException {
        ResultSet rs = metaData.getTables(this.getCatalog(), this.getSchemaPattern(), table, new String[]{"TABLE"});
        while (rs.next()) {
            TableInfo tableInfo = JSON.toJavaObject(getJsonObject(rs), TableInfo.class);
            tableInfo.setDatabaseVendorName(this.jdbcTemplate.getDatabaseConfig().getDatabase());
            tableList.add(tableInfo);
        }
    }

    @Override
    public List<ColumnInfo> getColumns(String... tableName) {

        return this.jdbcTemplate.execute((ConnectionCallback<List<ColumnInfo>>) conn -> {
            DatabaseMetaData metaData = conn.getMetaData();
            List<ColumnInfo> columnList = new ArrayList<>();

            if (tableName.length == 0) {
                getColumn(metaData, columnList, "%");
            } else {
                for (String name : tableName) {
                    getColumn(metaData, columnList, name);
                }
            }

            return columnList;
        });
    }

    private void getColumn(DatabaseMetaData metaData, List<ColumnInfo> columnList, String name) throws SQLException {
        try (ResultSet rs = metaData.getColumns(this.getCatalog(), this.getSchemaPattern(), name, "%")) {
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
    public List<PrimaryKey> getPrimaryKeys(String schema, String tableName) {
        return this.jdbcTemplate.execute((ConnectionCallback<List<PrimaryKey>>) conn -> {
            DatabaseMetaData metaData = conn.getMetaData();
            List<PrimaryKey> resultList = new ArrayList<>();

            try (ResultSet rs = metaData.getPrimaryKeys(this.getCatalog(), schema, tableName)) {
                while (rs.next()) {
                    resultList.add(JSON.toJavaObject(getJsonObject(rs), PrimaryKey.class));
                }
            }

            return resultList;
        });
    }

    @Override
    public List<ImportedKey> getImportedKeys(String schema, String tableName) {
        return this.jdbcTemplate.execute((ConnectionCallback<List<ImportedKey>>) conn -> {
            DatabaseMetaData metaData = conn.getMetaData();
            List<ImportedKey> resultList = new ArrayList<>();

            try (ResultSet rs = metaData.getImportedKeys(this.getCatalog(), schema, tableName)) {
                while (rs.next()) {
                    resultList.add(JSON.toJavaObject(getJsonObject(rs), ImportedKey.class));
                }
            }

            return resultList;
        });
    }

    @Override
    public List<ExportedKey> getExportedKeys(String schema, String tableName) {
        return this.jdbcTemplate.execute((ConnectionCallback<List<ExportedKey>>) conn -> {
            DatabaseMetaData metaData = conn.getMetaData();
            List<ExportedKey> resultList = new ArrayList<>();

            try (ResultSet rs = metaData.getExportedKeys(this.getCatalog(), schema, tableName)) {
                while (rs.next()) {
                    resultList.add(JSON.toJavaObject(getJsonObject(rs), ExportedKey.class));
                }
            }

            return resultList;
        });
    }


    @Override
    public List<VersionColumn> getVersionColumns(String schema, String tableName) {

        return this.jdbcTemplate.execute((ConnectionCallback<List<VersionColumn>>) conn -> {
            DatabaseMetaData metaData = conn.getMetaData();
            List<VersionColumn> resultList = new ArrayList<>();

            try (ResultSet rs = metaData.getVersionColumns(this.getCatalog(), schema, tableName)) {
                while (rs.next()) {
                    resultList.add(JSON.toJavaObject(getJsonObject(rs), VersionColumn.class));
                }
            }


            return resultList;
        });
    }

    @Override
    public List<Attribute> getAttributes(String... typeName) {

        return this.jdbcTemplate.execute((ConnectionCallback<List<Attribute>>) conn -> {
            DatabaseMetaData metaData = conn.getMetaData();
            List<Attribute> resultList = new ArrayList<>();

            if (typeName.length == 0) {
                getType(metaData, resultList, "%");
            } else {
                for (String name : typeName) {
                    getType(metaData, resultList, name);
                }
            }

            return resultList;
        });
    }

    private void getType(DatabaseMetaData metaData, List<Attribute> resultList, String name) throws SQLException {
        try (ResultSet rs = metaData.getAttributes(this.getCatalog(), this.getSchemaPattern(), name, "%")) {
            while (rs.next()) {
                resultList.add(JSON.toJavaObject(getJsonObject(rs), Attribute.class));
            }
        }
    }

    @Override
    public List<IndexInfo2> getIndexInfo(String schema, String tableName) {

        List<IndexInfo2> indexInfoList = this.jdbcTemplate.execute((ConnectionCallback<List<IndexInfo2>>) conn -> {
            DatabaseMetaData metaData = conn.getMetaData();
            List<IndexInfo2> resultList = new ArrayList<>();

            try (ResultSet rs = metaData.getIndexInfo(this.getCatalog(), schema, tableName, false, false)) {
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
    public int getTableCount(String schema, String tableName) {

        Integer tableCount = this.jdbcTemplate.execute((ConnectionCallback<Integer>) conn -> {
            DatabaseMetaData metaData = conn.getMetaData();
            int count = 0;

            try (ResultSet rs = metaData.getTables(conn.getCatalog(), schema, tableName, new String[]{"TABLE"})) {
                while (rs.next()) {
                    count++;
                }
            }

            return count;
        });

        // Unnecessary unboxing....
        return tableCount == null ? 0 : tableCount;
    }


    /**
     * 获取表的行的数量
     *
     * @param tableName 表名
     * @return 数量
     */
    @Override
    public long getTableRowCount(String schema, String tableName) {
        List<LinkedHashMap<String, Object>> queryResponse = this.jdbcTemplate.query("SELECT COUNT(*) COUNT FROM " + tableName);
        LinkedHashMap<String, Object> map = queryResponse.get(0);
        Object value = map.get("COUNT");
        return NumberUtils.toLong(value == null ? map.get("count") : value);
    }

    /**
     * 删除表
     *
     * @param tableName 表名
     */
    @Override
    public void dropTable(String tableName) {
        this.jdbcTemplate.execute("DROP TABLE " + tableName);
    }

    @Override
    public void deleteTable(String tableName) {
        this.jdbcTemplate.execute("DELETE FROM " + tableName);
    }

    /**
     * 截断表
     *
     * @param tableName 表名
     */
    @Override
    public void truncateTable(String tableName) {
        this.jdbcTemplate.execute("TRUNCATE TABLE " + tableName);
    }

    /**
     * 创建表
     *
     * @param tableName 表名
     */
    @Override
    public void createMetadataTable(String tableName) {
        String sqlBuilder = "CREATE TABLE " + tableName + "( " +
                "metadata_id    varchar(" + UUID.randomUUID().toString().length() + ") unique," +
                "table_name     varchar(128)," +
                "type           varchar(32)," +
                "statement      varchar(1000)," +
                "is_used        int" +
                ")";
        this.jdbcTemplate.execute(sqlBuilder);
    }

    /**
     * 更新元数据
     *
     * @param entity 实体
     * @return 1 成功；0 失败
     */
    @Override
    public int updateMetadata(SlaveMetaDataEntity entity) {
        String sqlBuilder = "UPDATE " + SlaveMetaDataEntity.TABLE_NAME +
                " SET is_used = ? where metadata_id = ?";
        return this.jdbcTemplate.update(sqlBuilder, entity.getIsUsed(), entity.getMetadataId());
    }

    /**
     * 插入源数据
     *
     * @param entity 实体
     * @return 1 成功；0 失败
     */
    @Override
    public int insertMetadata(SlaveMetaDataEntity entity) {
        String sqlBuilder = "INSERT INTO " + SlaveMetaDataEntity.TABLE_NAME +
                " (metadata_id, table_name, type, statement, is_used) VALUES (?, ?, ?, ?, ?)";
        return this.jdbcTemplate.update(sqlBuilder,
                entity.getMetadataId(), entity.getTableName(), entity.getType(), entity.getStatement(), entity.getIsUsed());
    }

    /**
     * 删除源数据
     *
     * @param tableName 表名
     * @return 1 成功；0 失败
     */
    @Override
    public int deleteMetadata(String tableName) {
        String sqlBuilder = "DELETE FROM " + SlaveMetaDataEntity.TABLE_NAME + " WHERE table_name = ?";
        return this.jdbcTemplate.update(sqlBuilder, tableName);
    }

    @Override
    public List<SlaveMetaDataEntity> getSlaveMetadataList(Integer isUsed, String... tableNames) {

        StringBuilder sBuilder = new StringBuilder("SELECT * FROM " + SlaveMetaDataEntity.TABLE_NAME);
        sBuilder.append(" WHERE ");
        Object[] params = new Object[tableNames.length + 1];
        int i = 0;
        if (tableNames.length > 0) {
            sBuilder.append("TABLE_NAME IN(");

            for (String tableName : tableNames) {
                sBuilder.append("?,");
                tableNames[i] = tableName;
                i++;
            }
            sBuilder.delete(sBuilder.length() - 1, sBuilder.length()).append(") AND");
        }
        params[i] = isUsed;

        sBuilder.append(" is_used = ?");

        return this.jdbcTemplate.queryForList2(sBuilder.toString(), SlaveMetaDataEntity.class, params);
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
}
