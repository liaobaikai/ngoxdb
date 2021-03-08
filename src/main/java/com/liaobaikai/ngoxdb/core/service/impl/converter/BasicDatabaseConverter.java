package com.liaobaikai.ngoxdb.core.service.impl.converter;

import com.liaobaikai.ngoxdb.bean.info.ColumnInfo;
import com.liaobaikai.ngoxdb.bean.info.ConstraintInfo;
import com.liaobaikai.ngoxdb.bean.info.IndexInfo2;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.bean.rs.ExportedKey;
import com.liaobaikai.ngoxdb.bean.rs.ImportedKey;
import com.liaobaikai.ngoxdb.bean.rs.PrimaryKey;
import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
import com.liaobaikai.ngoxdb.core.config.ConfigManager;
import com.liaobaikai.ngoxdb.core.config.DatabaseConfig;
import com.liaobaikai.ngoxdb.core.entity.SlaveMetaDataEntity;
import com.liaobaikai.ngoxdb.core.enums.fn.DatabaseFunctionEnum;
import com.liaobaikai.ngoxdb.core.listener.OnMigrateTableDataListener;
import com.liaobaikai.ngoxdb.core.service.DatabaseComparator;
import com.liaobaikai.ngoxdb.core.service.DatabaseConverter;
import com.liaobaikai.ngoxdb.utils.StringUtils;

import java.sql.Clob;
import java.sql.SQLException;
import java.util.*;

/**
 * @author baikai.liao
 * @Time 2021-01-28 22:17:26
 */
public abstract class BasicDatabaseConverter implements DatabaseConverter {

    // 函数映射
    private final Map<DatabaseFunctionEnum, String[]> databaseFunctionMap = new HashMap<>();

    private final String masterDatabaseVendor;
    private final boolean isMaster;
    private final JdbcTemplate2 jdbcTemplate;
    private final DatabaseConfig databaseConfig;
    private int tableNameMaxLength;

    /**
     * 表的行数
     */
    private final Map<String, Long> tableRowCount = new HashMap<>();

    /**
     * 已转换的表
     */
    protected final List<String> convertFailTableList = new ArrayList<>();

    public BasicDatabaseConverter() {
        // 默认实现
        this(null, false, null, null);
    }

    public BasicDatabaseConverter(JdbcTemplate2 jdbcTemplate,
                                  boolean isMaster,
                                  String masterDatabaseVendor,
                                  DatabaseConfig databaseConfig) {
        this.jdbcTemplate = jdbcTemplate;
        this.isMaster = isMaster;
        this.masterDatabaseVendor = masterDatabaseVendor;
        this.databaseConfig = databaseConfig;
    }

    /**
     * 添加函数映射
     *
     * @param map Map<DatabaseFunctionEnum, String[]>
     */
    public abstract void buildDatabaseFunctionMap(Map<DatabaseFunctionEnum, String[]> map);

    /**
     * 处理字符串、数字、日期的长度、精度问题
     */
    protected abstract void handleDataType(StringBuilder sqlBuilder, ColumnInfo columnInfo);

    /**
     * 处理异常信息
     *
     * @param e 异常对象
     */
    protected void handleException(Throwable e) {
        e.printStackTrace();
        getLogger().error("[{}] \033[31;42m {} \033[m", this.getDatabaseConfig().getName(), e.getMessage());
    }

    @Override
    public boolean isMaster() {
        return this.isMaster;
    }

    @Override
    public String getMasterDatabaseVendor() {
        return this.masterDatabaseVendor;
    }

    @Override
    public DatabaseConfig getDatabaseConfig() {
        return this.databaseConfig;
    }

    @Override
    public List<String> getConvertFailTableList() {
        return this.convertFailTableList;
    }

    @Override
    public String getDatabaseDataTypeDefault(String masterDataTypeDef, int dataType) {
        return ConfigManager.getDatabaseDataTypeDefault(masterDataTypeDef, this.masterDatabaseVendor, this.getDatabaseVendor());
    }

    @Override
    public Map<DatabaseFunctionEnum, String[]> getDatabaseFunctionMap() {
        this.buildDatabaseFunctionMap(this.databaseFunctionMap);
        return this.databaseFunctionMap;
    }


    /**
     * 清空删除元数据
     */
    protected void truncateMetadata() {
        if (!isMaster) {
            if (this.getDatabaseDao().getTableCount(null, SlaveMetaDataEntity.TABLE_NAME) == 0) {
                // 表不存在，先创建
                this.getDatabaseDao().createMetadataTable(this.getRightName(SlaveMetaDataEntity.TABLE_NAME));
            } else {
                // 清空表信息
                this.getDatabaseDao().truncateTable(this.getRightName(SlaveMetaDataEntity.TABLE_NAME));
            }
        }
    }

    /**
     * 判断数据库厂家是否相同
     *
     * @return boolean
     */
    public boolean isSameDatabaseVendor() {
        if (this.isMaster) {
            return false;
        }
        return this.masterDatabaseVendor.equals(this.getDatabaseVendor());
    }

    /**
     * 获取JdbcTemplate2
     */
    public JdbcTemplate2 getJdbcTemplate() {
        return this.jdbcTemplate;
    }

    /**
     * 创建表之前的操作
     *
     * @param ti 表信息
     * @return 是否继续创建表
     */
    public boolean beforeCreateTable(TableInfo ti) {
        // 查询表是否存在
        String tName = this.getRightName(ti.getTableName());
        if (this.getDatabaseDao().getTableCount(this.isSameDatabaseVendor() ? ti.getTableSchem() : null, ti.getTableName()) > 0) {
            if (this.getDatabaseConfig().isReplaceTable()) {
                getLogger().warn("[{}] Drop table if exists {}", this.getDatabaseConfig().getName(), tName);
                this.getDatabaseDao().dropTable(tName);
            } else {
                getLogger().warn("[{}] Table {} already exists", this.getDatabaseConfig().getName(), tName);
                return false;
            }
        }
        return true;
    }

    /**
     * 创建一个表之前的操作
     *
     * @param ti 表信息
     */
    public String buildCreateTable(TableInfo ti) {

        StringBuilder sqlBuilder = new StringBuilder("CREATE TABLE ");
        sqlBuilder.append(this.getRightName(ti.getTableName())).append(" (");

        // 列信息
        ti.getColumns().forEach(columnInfo -> {

            sqlBuilder.append(getRightName(columnInfo.getColumnName())).append(" ");

            if (this.isSameDatabaseVendor()) {
                // 相同的数据库厂家
                sqlBuilder.append(columnInfo.getColumnType());
            } else {
                // 其他数据库厂家
                this.handleDataType(sqlBuilder, columnInfo);
            }

            sqlBuilder.append(" ");
            if (columnInfo.isNotNull()) {
                sqlBuilder.append("NOT NULL ");
            }

            // 默认值
            if (columnInfo.getColumnDef() != null) {
                // 时间默认值
                String dataTypeDef = this.getDatabaseDataTypeDefault(columnInfo.getColumnDef(), columnInfo.getDataType());
                sqlBuilder.append("DEFAULT ").append(dataTypeDef == null ? columnInfo.getColumnDef() : dataTypeDef).append(" ");
            }

            sqlBuilder.append(",");

        });

        // 主键
        buildPrimaryKeys(ti, sqlBuilder);
        // 约束
        buildConstraintInfo(ti, sqlBuilder);

        sqlBuilder.delete(sqlBuilder.length() - 1, sqlBuilder.length()).append(") ");

        return sqlBuilder.toString();
    }

    /**
     * 创建一个表之后的操作
     *
     * @param ti 表信息
     */
    public void afterCreateTable(TableInfo ti) {

        // 外键
        this.buildForeignKeys(ti);

        // 索引 & 唯一键
        this.buildIndex(ti);

        // 注释
        this.buildComment(ti);
    }

    @Override
    public List<TableInfo> getTableInfo(String... tableName) {
        return this.buildTableInfo(tableName);
    }

    /**
     * 构建TableInfo对象
     *
     * @param tableName 表名
     * @return List<TableInfo>
     */
    public List<TableInfo> buildTableInfo(String... tableName) {

        this.getDatabaseDao().getDatabaseInfo();

        //  from jdbc
        // 表
        List<TableInfo> tables = getTables(tableName);
        tables.removeIf(tableInfo -> tableInfo.getTableName().equalsIgnoreCase(SlaveMetaDataEntity.TABLE_NAME));

        if (tables.size() == 0) {
            return tables;
        }

        for (final TableInfo tableInfo : tables) {

            final String table = tableInfo.getTableName();

            if(table.length() > this.tableNameMaxLength){
                this.tableNameMaxLength = table.length();
            }

            getLogger().info("Collecting table {} ...", table);

            // from jdbc.....
            // 列信息
            // 外键
            tableInfo.setColumns(this.getColumns(table));
            // 主键
            // 获取唯一键
            tableInfo.setPrimaryKeys(this.getPrimaryKeys(tableInfo.getTableSchem(), table));
            tableInfo.getPrimaryKeys().forEach(primaryKey -> tableInfo.getUniqueKeys().add(primaryKey.getColumnName()));

            // 索引信息
            tableInfo.setIndexInfo(this.getIndexInfo2(tableInfo.getTableSchem(), table));
            // 其他表引用本表的键（外键）
            // tableInfo.setExportedKeys(this.getExportedKeys(tableInfo.getTableSchem(), table));
            // 本表引用其他表的键（外键）
            tableInfo.setImportedKeys(this.getImportedKeys(tableInfo.getTableSchem(), table));
            // from jdbc end...

            // 约束
            tableInfo.setConstraintInfo(this.getConstraintInfo(table));
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

        return tables;
    }


    /**
     * 获取表的约束信息
     *
     * @param tableName 表名
     * @return List<ConstraintInfo>
     */
    public List<ConstraintInfo> getConstraintInfo(String... tableName) {
        return this.getDatabaseDao().getConstraintInfo(tableName);
    }

    /**
     * 获取本表所有外键
     *
     * @param schema    模式
     * @param tableName 表名
     * @return List<ImportedKey>
     */
    public List<ImportedKey> getImportedKeys(String schema, String tableName) {
        return this.getDatabaseDao().getImportedKeys(schema, tableName);
    }

    /**
     * 获取本表被引用的键（外键）
     *
     * @param schema    模式
     * @param tableName 表名
     * @return List<ExportedKey>
     */
    public List<ExportedKey> getExportedKeys(String schema, String tableName) {
        return this.getDatabaseDao().getExportedKeys(schema, tableName);
    }

    /**
     * 获取索引信息
     *
     * @param schema    模式
     * @param tableName 表名
     * @return List<IndexInfo2>
     */
    public List<IndexInfo2> getIndexInfo2(String schema, String tableName) {
        return this.getDatabaseDao().getIndexInfo(schema, tableName);
    }

    /**
     * 获取所有主键
     *
     * @param schema    模式
     * @param tableName 表名
     * @return List<PrimaryKey>
     */
    public List<PrimaryKey> getPrimaryKeys(String schema, String tableName) {
        return this.getDatabaseDao().getPrimaryKeys(schema, tableName);
    }

    /**
     * 获取所有列
     *
     * @param tableName 表名
     * @return List<ColumnInfo>
     */
    public List<ColumnInfo> getColumns(String... tableName) {
        return this.getDatabaseDao().getColumns(tableName);
    }

    /**
     * 获取所有表
     *
     * @param tableName 表名
     * @return List<TableInfo>
     */
    public List<TableInfo> getTables(String... tableName) {
        return this.getDatabaseDao().getTables(tableName);
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
                    StringUtils.escape(this.getRightName(columnInfo.getColumnName())),
                    columnInfo.getColumnName());
        }
        ci.setCheckCondition(checkCondition);
    }

    @Override
    public void createAllTable(List<TableInfo> tis) {
        for (TableInfo ti : tis) {
            try {
                this.createTable(ti);
            } catch (Exception e) {
                // java.sql.BatchUpdateException: General error
                convertFailTableList.add(ti.getTableName());
                this.handleException(e);
            }
        }
    }

    @Override
    public void createTable(TableInfo ti) {
        boolean isNextAction = this.beforeCreateTable(ti);
        if (!isNextAction) {
            return;
        }
        String ddl = this.buildCreateTable(ti);
        getLogger().info("[{}] {}", this.getDatabaseConfig().getName(), ddl);
        this.getDatabaseDao().getJdbcTemplate().execute(ddl);
        this.afterCreateTable(ti);
    }


    /**
     * 构造约束信息
     *
     * @param ti         表信息
     * @param sqlBuilder Builder
     */
    public void buildConstraintInfo(TableInfo ti, StringBuilder sqlBuilder) {
        if (ti.getConstraintInfo().size() == 0) {
            return;
        }

        // 约束
        ti.getConstraintInfo().forEach(constraintInfo -> {
            sqlBuilder.append("CONSTRAINT ").append(getRightName(constraintInfo.getConstraintName())).append(" ");
            if (constraintInfo.getConstraintType().equalsIgnoreCase(ConstraintInfo.TYPE_CHECK_CONSTRAINT)) {
                sqlBuilder.append("CHECK ");
            }
            sqlBuilder.append(constraintInfo.getCheckCondition()).append(",");
        });
    }

    /**
     * 构造主键信息
     * 原则上，一个表只有一个主键，主键可以包含一列或多列
     * 将List<PrimaryKey>转成字符串， 如：PRIMARY KEY (col1, col2, col3, ...)
     *
     * @param ti         表信息
     * @param sqlBuilder Builder
     */
    public void buildPrimaryKeys(TableInfo ti, StringBuilder sqlBuilder) {
        if (ti.getPrimaryKeys().size() == 0) {
            return;
        }

        sqlBuilder.append("PRIMARY KEY (");
        // 再次排序
        ti.getPrimaryKeys().sort(Comparator.comparing(PrimaryKey::getKeySeq));
        ti.getPrimaryKeys().forEach(pk -> sqlBuilder.append(getRightName(pk.getColumnName())).append(","));
        sqlBuilder.delete(sqlBuilder.length() - 1, sqlBuilder.length()).append("),");
    }

    /**
     * 构造外键，并保存到元数据表
     *
     * @param ti 表信息
     */
    public void buildForeignKeys(TableInfo ti) {

        if (ti == null || ti.getImportedKeys() == null) {
            return;
        }

        StringBuilder sBuilder = new StringBuilder();
        for (ImportedKey importedKey : ti.getImportedKeys()) {

            sBuilder.append("ALTER TABLE ").append(this.getRightName(importedKey.getFkTableName()));
            sBuilder.append(" ADD CONSTRAINT ").append(this.getRightName(importedKey.getFkName()));
            sBuilder.append(" FOREIGN KEY ( ").append(this.getRightName(importedKey.getFkColumnName())).append(" ) ");
            sBuilder.append(" REFERENCES ").append(this.getRightName(importedKey.getPkTableName())).append(" (")
                    .append(this.getRightName(importedKey.getPkColumnName())).append(" ) ");

            // 更新规则
            buildForeignKeyChangeRules(sBuilder, importedKey);

            this.getDatabaseDao().insertMetadata(new SlaveMetaDataEntity(ti.getTableName(), SlaveMetaDataEntity.TYPE_FOREIGN_KEY, sBuilder.toString()));

            // 清空缓冲区
            sBuilder.delete(0, sBuilder.length());
        }
    }

    /**
     * 更新规则，如果数据库不支持的话，需要重写。
     *
     * @param sBuilder    语句
     * @param importedKey 外键信息
     */
    protected void buildForeignKeyChangeRules(StringBuilder sBuilder,
                                              ImportedKey importedKey) {

        // 删除规则
        sBuilder.append(" ").append(importedKey.getDeleteAction()).append(" ");
        // 更新规则
        sBuilder.append(" ").append(importedKey.getUpdateAction()).append(" ");

    }

    /**
     * 构造索引，并保存到元数据表
     *
     * @param ti 表信息
     */
    public void buildIndex(TableInfo ti) {
        // oracle:
        // https://docs.oracle.com/en/database/oracle/oracle-database/21/admin/managing-indexes.html#GUID-2CE1BB91-3EFA-450D-BD31-0C961549F0C2
        // https://docs.oracle.com/cd/B19306_01/server.102/b14200/statements_5010.htm
        // 位图索引不支持。
        // CREATE [ UNIQUE | BITMAP ] INDEX [ schema. ]index
        //   ON { cluster_index_clause
        //      | table_index_clause
        //      | bitmap_join_index_clause
        //      } ;

        // pgsql:
        // // www.postgres.cn/docs/9.4/indexes-types.html

        if (ti == null || ti.getIndexInfo() == null) {
            return;
        }

        // 一个键名，多个列
        StringBuilder sBuilder = null;
        List<String> buildIndex = null;
        String indexType;

        for (IndexInfo2 ii : ti.getIndexInfo()) {

            if (ii.isTableIndexStatistic()) {
                // 统计信息不需要。
                continue;
            }

            if (sBuilder == null) {
                sBuilder = new StringBuilder();
            }
            if (buildIndex == null) {
                buildIndex = new ArrayList<>();
            }

            if (buildIndex.contains(ii.getIndexName())) {
                continue;
            }

            if(ii.getIndexName().equals("fk_film_category_category")){
                System.out.println();
            }

            buildIndex.add(ii.getIndexName());

            sBuilder.append("CREATE ");

            if (!ii.isNonUnique()) {
                indexType = SlaveMetaDataEntity.TYPE_UNIQUE_INDEX;
                sBuilder.append("unique ");
            }
            /*else if (IndexTypeEnum.FULLTEXT.toString().equalsIgnoreCase(ii.getIndexTypeDesc())) {
                // 全文索引
                indexType = SlaveMetaDataEntity.TYPE_FULLTEXT_INDEX;
                sBuilder.append("FULLTEXT ");
            } else if(IndexTypeEnum.SPATIAL.toString().equalsIgnoreCase(ii.getIndexTypeDesc())){
                // 空间索引
                indexType = SlaveMetaDataEntity.TYPE_SPATIAL_INDEX;
                sBuilder.append("SPATIAL ");
            } */
            else {
                indexType = SlaveMetaDataEntity.TYPE_INDEX;
            }

            sBuilder.append("INDEX ");

            // 有些数据库创建外键的同时，会创建一个索引。索引名称和外键的名称一样。
            // [index_name]
            String indexName = ii.getIndexName();
            if (this.getDatabaseConfig().isGenerateName()) {
                // 自动生成
                indexName = this.buildIndexName(ii, ti);
            }

            sBuilder.append(this.getRightName(indexName)).append(" ON ").append(this.getRightName(ii.getTableName()));

            // column-list
            sBuilder.append(" (");

            // 获取所有的columnName
            for (IndexInfo2 ii2 : ti.getIndexInfo()) {
                if (!ii.getIndexName().equals(ii2.getIndexName())) {
                    continue;
                }
                // sBuilder.append(this.getRightName(ii2.getColumnName())).append(" ").append(ii2.getOrder()).append(",");
                sBuilder.append(ii2.getColumnName()).append(" ").append(ii2.getOrder()).append(",");
            }

            sBuilder.delete(sBuilder.length() - 1, sBuilder.length()).append(")");

            this.getDatabaseDao().insertMetadata(new SlaveMetaDataEntity(ti.getTableName(), indexType, sBuilder.toString()));

            // 全部清掉
            sBuilder.delete(0, sBuilder.length());
        }
    }

    /**
     * 索引名称
     *
     * @param ii 索引
     * @param ti 表信息
     * @return 索引名称
     */
    protected String buildIndexName(IndexInfo2 ii, TableInfo ti) {

        StringBuilder indexNameBuilder = new StringBuilder();
        final int indexNameMaxLength = 64;

        if (ii.isNonUnique()) {
            indexNameBuilder.append("idx_");
        } else {
            indexNameBuilder.append("ux_");
        }
        indexNameBuilder.append(ti.getTableName().toLowerCase()).append("_");

        StringBuilder columnBuilder = new StringBuilder();
        // 获取所有的columnName
        for (IndexInfo2 ii2 : ti.getIndexInfo()) {
            if (!ii.getIndexName().equals(ii2.getIndexName())) {
                continue;
            }
            columnBuilder.append(ii2.getColumnName().toLowerCase()).append("_");
        }
        columnBuilder.delete(columnBuilder.length() - 1, columnBuilder.length());

        if (ti.getIndexNames() == null) {
            ti.setIndexNames(new ArrayList<>());
        }

        // 最长64个字符、
        if ((indexNameBuilder.length() + columnBuilder.length()) > indexNameMaxLength) {
            // 组装所有列的时候，超过长度限制。
            int len = indexNameMaxLength - indexNameBuilder.length();
            int i = 1;
            do {
                if (len > 10) {
                    indexNameBuilder.append("comb_cols_").append(i);
                } else if (len >= 6) {
                    indexNameBuilder.append("cols_").append(i);
                } else if (len > 1) {
                    indexNameBuilder.append("_").append(i);
                } else {
                    break;
                }
                i++;
            } while (ti.getIndexNames().contains(indexNameBuilder.toString()));

        } else {
            indexNameBuilder.append(columnBuilder);
        }

        ti.getIndexNames().add(indexNameBuilder.toString());
        return indexNameBuilder.toString();
    }


    /**
     * 构造表注释、列注释
     *
     * @param ti 表信息
     */
    public void buildComment(TableInfo ti) {
        // oracle:
        // https://docs.oracle.com/en/database/oracle/oracle-database/12.2/sqlrf/COMMENT.html#GUID-65F447C4-6914-4823-9691-F15D52DB74D7
        // To add a comment to a table, view, or materialized view, you must have COMMENT ANY TABLE system privilege.
        //
        // pgsql:
        // http://postgres.cn/docs/9.4/sql-comment.html
        //

        final String tName = ti.getTableName();

        // 列注释
        ti.getColumns().forEach(c -> {
            if (StringUtils.isEmpty(c.getRemarks())) {
                return;
            }

            String s = String.format("COMMENT ON COLUMN %s.%s IS '%s'",
                    this.getRightName(tName),
                    this.getRightName(c.getColumnName()),
                    c.getRemarks());
            this.getDatabaseDao().insertMetadata(new SlaveMetaDataEntity(tName, SlaveMetaDataEntity.TYPE_COMMENT, s));

        });

        // 表注释
        if (StringUtils.isNotEmpty(ti.getRemarks())) {

            String s = String.format("COMMENT ON TABLE %s IS '%s'",
                    this.getRightName(tName),
                    ti.getRemarks());
            this.getDatabaseDao().insertMetadata(new SlaveMetaDataEntity(tName, SlaveMetaDataEntity.TYPE_COMMENT, s));
        }
    }


    @Override
    public int getCharBytes() {
        return this.getDatabaseDao().getDatabaseInfo().getMaxLen();
    }

    @Override
    public void migrateAllTable(List<TableInfo> tis,
                                OnMigrateTableDataListener migrateTableDataListener) {
        for (TableInfo ti : tis) {
            this.migrateTableData(ti, migrateTableDataListener);
        }
    }

    @Override
    public void migrateTableData(TableInfo ti,
                                 OnMigrateTableDataListener migrateTableDataListener) {

        final String table = this.getRightName(ti.getTableName());

        // 迁移表的数据
        // 查询原系统的数据
        int pageSize = this.getDatabaseConfig().getPageSize();
        // 获取表的行数
        long tableRowCount = this.getDatabaseDao().getTableRowCount(ti.getTableSchem(), table);
        migrateTableDataListener.onBeforeMigrateTableData(ti, tableRowCount, pageSize);

        if (tableRowCount == 0) {
            // 无数据
            getLogger().info("[{}] Table {} is empty", this.getDatabaseConfig().getName(), ti.getTableName());
            return;
        }

        // 分页插入
        // 计算表的页数
        long tablePageSize = (tableRowCount / pageSize) + (tableRowCount % pageSize > 0 ? 1 : 0);
        for (int i = 0, offset, limit; i < tablePageSize; i++) {
            // 循环获取每一个分页的数据
            offset = i * pageSize;
            limit = (i != tablePageSize - 1) ? pageSize : (int) (tableRowCount - (long) offset);

            List<Object[]> batchArgs = this.queryData(this.getPaginationSQL(ti, offset, limit));
            migrateTableDataListener.onMigrateTableData(batchArgs, ti, i + 1);
        }

        migrateTableDataListener.onAfterMigrateTableData(ti, tablePageSize);
    }

    @Override
    public void beforeMigrateTableData(TableInfo ti) {
    }

    /**
     * 迁移数据后需要做的动作
     * @param ti 表信息
     * @param tablePageSize 表的总行数
     */
    @Override
    public void afterMigrateTableData(TableInfo ti, long tablePageSize) {
    }


    @Override
    public List<Object[]> queryAllData(TableInfo ti) {
        return this.queryData("SELECT * FROM " + this.getRightName(ti.getTableName()) + " ORDER BY " + this.getTableOrderColumnNames(ti.getColumns()));
    }


    @Override
    public List<Object[]> queryData(String querySql) {
        return this.queryData(querySql, null);
    }

    @Override
    public List<Object[]> queryData(String querySql, Object[] paramValues) {

        getLogger().info("[{}] {}", this.getDatabaseConfig().getName(), querySql);

        // 查询某一页的所有的数据
        return this.getDatabaseDao().getJdbcTemplate().query(querySql, paramValues, (rs, rowNum) -> {
            Object[] values = new Object[rs.getMetaData().getColumnCount()];
            for (int i = 0, len = values.length; i < len; i++) {
                // 源数据库处理为通用类型
                Object value = rs.getObject(i + 1);
                if (value instanceof Clob) {
                    // clob & nclob
                    try {
                        values[i] = StringUtils.clob2String((Clob) value);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    continue;
                }
                values[i] = this.convertData(value);
            }
            return values;
        });
    }

    @Override
    public List<Object[]> queryPaginationData(TableInfo ti, int offset, int limit) {
        return this.queryData(this.getPaginationSQL(ti, offset, limit));
    }

    @Override
    public List<Object[]> queryInData(TableInfo ti, String columnName, Object[] inputData) {

        // build sql
        // such as: select * from table where col in (?, ?, ?,...)
        StringBuilder sqlBuilder = new StringBuilder("SELECT * FROM ");
        sqlBuilder.append(this.getRightName(ti.getTableName()));
        sqlBuilder.append(" WHERE ").append(this.getRightName(columnName)).append(" IN (");
        for (int i = 0, len = inputData.length; i < len; i++) {
            sqlBuilder.append("?");
            if (i != len - 1) {
                sqlBuilder.append(",");
            }
        }
        sqlBuilder.append(")");

        return this.queryData(sqlBuilder.toString(), inputData);
    }

    @Override
    public void postData(List<Object[]> batchArgs, TableInfo ti) {

        // 处理行数据
        // 目标数据库不支持的类型需要额外处理。
        this.changeBatchArgs(batchArgs, ti);

        // sqlserver:
        // 将截断字符串或二进制数据
        // https://support.microsoft.com/en-us/topic/kb4468101-improvement-optional-replacement-for-string-or-binary-data-would-be-truncated-message-with-extended-information-in-sql-server-2016-and-2017-a4279ad6-1d3b-3960-77ef-c82a909f4b89
        // 批量插入

        final String sql = this.buildInsertSQL(ti);

        try {
            this.persistent(sql, batchArgs, ti);
        } catch (Exception e) {
            this.handleException(e);
        }
    }

    /**
     * 构建插入的SQL语句
     *
     * @param ti 表信息
     * @return insert into T (col1, col2, col3, ...) values (?, ?, ?, ...)
     */
    protected String buildInsertSQL(TableInfo ti) {
        // 生成sql语句：
        // insert into T (col1, col2, col3, ...) values (?, ?, ?, ...)
        final StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ").append(this.getRightName(ti.getTableName())).append(" (");
        final StringBuilder sqlPlaceHolderBuilder = new StringBuilder();

        // int i = 0;
        // int[] argTypes = new int[ti.getColumns().size()];
        for (ColumnInfo c : ti.getColumns()) {
            sqlBuilder.append(this.getRightName(c.getColumnName())).append(",");
            sqlPlaceHolderBuilder.append("?,");
            // argTypes[i++] = converter.changeDataType(c.getDataType());
        }

        sqlBuilder.delete(sqlBuilder.length() - 1, sqlBuilder.length()).append(") VALUES (");
        sqlPlaceHolderBuilder.delete(sqlPlaceHolderBuilder.length() - 1, sqlPlaceHolderBuilder.length());
        sqlBuilder.append(sqlPlaceHolderBuilder).append(")");
        return sqlBuilder.toString();
    }

    @Override
    public Object convertData(Object srcObj) throws SQLException {
        return srcObj;
    }

    /**
     * 将数据保存到数据库
     *
     * @param sql       insert into table (col1, col2, col3,...) values (?, ?, ?,...)
     * @param batchArgs 每行的数据
     * @param ti        表信息
     */
    public void persistent(String sql, List<Object[]> batchArgs, TableInfo ti) {

        getLogger().info("[{}] {}", this.getDatabaseConfig().getName(), sql);

        this.getDatabaseDao().getJdbcTemplate().batchUpdate(sql, batchArgs);

        // 表的行数统计
        Long count = this.getTableRowCount().get(ti.getTableName());
        if(count == null){
            count = 0L;
        }
        count += (long) batchArgs.size();
        this.getTableRowCount().put(ti.getTableName(), count);

        getLogger().info("[{}] Table {} {} rows created", this.getDatabaseConfig().getName(), ti.getTableName(), batchArgs.size());
    }

    @Override
    public void postMetadata() {
        // 应用元数据信息
        List<SlaveMetaDataEntity> list = this.getDatabaseDao().getSlaveMetadataList(SlaveMetaDataEntity.NOT_USED);

        for (SlaveMetaDataEntity entity : list) {
            // 执行语句
            try {
                getLogger().info("[{}] {}", this.getDatabaseConfig().getName(), entity.getStatement());
                this.getDatabaseDao().getJdbcTemplate().execute(entity.getStatement());
                // 更新状态，是否执行过
                entity.setIsUsed(SlaveMetaDataEntity.USED);
                this.getDatabaseDao().updateMetadata(entity);
            } catch (Exception e) {
                this.handleException(e);
            }
        }
    }

    /**
     * 获取所有列名
     *
     * @param tableColumns 列
     * @return col1, col2, col3, ...
     */
    protected String getTableColumnNames(List<ColumnInfo> tableColumns) {
        return getTableColumnNames(tableColumns, null);
    }

    /**
     * 获取所有列名
     *
     * @param tableColumns 列
     * @param tableAlias   表别名
     * @return col1, col2, col3, ...
     */
    protected String getTableColumnNames(List<ColumnInfo> tableColumns, String tableAlias) {
        StringBuilder sBuilder = new StringBuilder();
        for (ColumnInfo columnInfo : tableColumns) {
            if (tableAlias != null) {
                sBuilder.append(tableAlias).append(".");
            }
            sBuilder.append(this.getRightName(columnInfo.getColumnName())).append(",");
        }
        return sBuilder.delete(sBuilder.length() - 1, sBuilder.length()).toString();
    }


    /**
     * 将列表转成字符串，以,号分割
     *
     * @param list 列表
     * @return a1, a2, a3, ...
     */
    protected String getTableUniqueKeys(List<String> list) {
        StringBuilder result = new StringBuilder();
        for (int i = 0, len = list.size(); i < len; i++) {
            result.append(this.getRightName(list.get(i)));
            if (i != len - 1) {
                result.append(",");
            }
        }
        return result.toString();
    }

    /**
     * 获取表的可以排序的列名，如clob类型的列不支持
     *
     * @param tableColumns 所有列
     * @return col1, col2, col3, ...
     */
    protected String getTableOrderColumnNames(List<ColumnInfo> tableColumns) {
        StringBuilder sBuilder = new StringBuilder();
        for (ColumnInfo columnInfo : tableColumns) {
            switch (columnInfo.getDataType()) {
                case java.sql.Types.BLOB:
                case java.sql.Types.BINARY:
                case java.sql.Types.VARBINARY:
                case java.sql.Types.CLOB:
                case java.sql.Types.NCLOB:
                case java.sql.Types.LONGNVARCHAR:
                case java.sql.Types.LONGVARCHAR:
                case java.sql.Types.LONGVARBINARY:
                    continue;
                case java.sql.Types.CHAR:
                case java.sql.Types.NCHAR:
                case java.sql.Types.VARCHAR:
                case java.sql.Types.NVARCHAR:
                    if (columnInfo.getColumnSize() > 255) {
                        // access database varchar最大是255，因此为了兼容其他数据库，大于255字段的都需要排除。
                        continue;
                    }
                    break;
            }
            sBuilder.append(this.getRightName(columnInfo.getColumnName())).append(",");
        }
        return sBuilder.delete(sBuilder.length() - 1, sBuilder.length()).toString();
    }

    @Override
    public void changeBatchArgs(List<Object[]> batchArgs, TableInfo ti) {

    }

    @Override
    public Map<String, Long> getTableRowCount() {
        return this.tableRowCount;
    }

    @Override
    public int getTableNameMaxLength() {
        return this.tableNameMaxLength;
    }

    /**
     * 获取比较器
     *
     * @return 比较器
     */
    public abstract DatabaseComparator getComparator();


}
