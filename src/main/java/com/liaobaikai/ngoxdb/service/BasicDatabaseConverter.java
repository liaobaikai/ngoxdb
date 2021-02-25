package com.liaobaikai.ngoxdb.service;

import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
import com.liaobaikai.ngoxdb.config.ConfigManager;
import com.liaobaikai.ngoxdb.config.DatabaseConfig;
import com.liaobaikai.ngoxdb.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.entity.SlaveMetaDataEntity;
import com.liaobaikai.ngoxdb.enums.fn.DatabaseFunctionEnum;
import com.liaobaikai.ngoxdb.info.ColumnInfo;
import com.liaobaikai.ngoxdb.info.ConstraintInfo;
import com.liaobaikai.ngoxdb.info.IndexInfo2;
import com.liaobaikai.ngoxdb.info.TableInfo;
import com.liaobaikai.ngoxdb.rs.ExportedKey;
import com.liaobaikai.ngoxdb.rs.ImportedKey;
import com.liaobaikai.ngoxdb.rs.PrimaryKey;
import com.liaobaikai.ngoxdb.utils.CommonUtils;
import com.liaobaikai.ngoxdb.utils.StringUtils;

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
    /**
     * 已转换的表
     */
    protected final List<String> convertedFailTables = new ArrayList<>();

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

    protected void skip(String msg, Object...args){
        CommonUtils.skip(this.getDatabaseConfig(), msg, args);
    }

    protected void warn(String msg, Object...args){
        CommonUtils.warn(this.getDatabaseConfig(), msg, args);
    }

    protected void log(String msg, Object...args){
        CommonUtils.log(this.getDatabaseConfig(), msg, args);
    }

    protected void log2(String msg, Object...args){
        CommonUtils.log2(this.getDatabaseConfig(), msg, args);
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
    public String getDatabaseDataTypeDefault(String masterDataTypeDef, int dataType) {
        return ConfigManager.getDatabaseDataTypeDefault(masterDataTypeDef, this.masterDatabaseVendor, this.getDatabaseVendor());
    }

    @Override
    public Map<DatabaseFunctionEnum, String[]> getDatabaseFunctionMap() {
        this.buildDatabaseFunctionMap(this.databaseFunctionMap);
        return this.databaseFunctionMap;
    }


    /**
     * 添加函数映射
     * @param map Map<DatabaseFunctionEnum, String[]>
     */
    public abstract void buildDatabaseFunctionMap(Map<DatabaseFunctionEnum, String[]> map);

    /**
     * 清空删除元数据
     */
    protected void truncateMetadata(){
        if(!isMaster){
            if(this.getDatabaseDao().getTableCount(null, SlaveMetaDataEntity.TABLE_NAME) == 0) {
                // 表不存在，先创建
                this.getDatabaseDao().createMetadataTable(SlaveMetaDataEntity.TABLE_NAME);
            } else {
                // 清空表信息
                this.getDatabaseDao().truncateTable(SlaveMetaDataEntity.TABLE_NAME);
            }
        }
    }

    /**
     * 判断数据库厂家是否相同
     * @return boolean
     */
    public boolean isSameDatabaseVendor(){
        if(this.isMaster){
            return false;
        }
        return this.masterDatabaseVendor.equals(this.getDatabaseVendor());
    }

    /**
     * 获取JdbcTemplate2
     */
    public JdbcTemplate2 getJdbcTemplate(){
        return this.jdbcTemplate;
    }

    /**
     * 创建表之前的操作
     *
     * @param ti 表信息
     * @return 是否继续创建表
     */
    public boolean beforeCreateTable(TableInfo ti) {
        DatabaseConfig databaseConfig = this.getDatabaseDao().getJdbcTemplate().getDatabaseConfig();

        // 查询表是否存在
        String tName = this.getRightName(ti.getTableName());
        if(this.getDatabaseDao().getTableCount(this.isSameDatabaseVendor() ? ti.getTableSchem() : null, ti.getTableName()) > 0){

            if(databaseConfig.isReplaceTable()) {
                log2("Table {} already exists! Replace.", tName);
                log("DROP TABLE " + tName);
                this.getDatabaseDao().dropTable(tName);
                log2("Table {} removed.", tName);
            } else {
                skip("Table {} already exists.", tName);
                return false;
            }
        }

        return true;
    }

    /**
     * 创建一个表之前的操作
     * @param ti 表信息
     */
    public StringBuilder buildCreateTable(TableInfo ti){

        StringBuilder sqlBuilder = new StringBuilder(
                "CREATE TABLE " + this.getRightName(ti.getTableName()) + " ( "
        );

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

        return sqlBuilder;
    }

    /**
     * 创建一个表之后的操作
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
     * @param tableName 表名
     * @return List<TableInfo>
     */
    public List<TableInfo> buildTableInfo(String... tableName){

        this.getDatabaseDao().getDatabaseInfo();

        //  from jdbc
        // 表
        List<TableInfo> tables = getTables(tableName);
        // 列
        List<ColumnInfo> columns = getColumns(tableName);


        // sql查询
        // 约束
        List<ConstraintInfo> constraintInfo = getConstraintInfo(tableName);

        tables.forEach(tableInfo -> {

            // from jdbc.....
            // 列信息
            tableInfo.setColumns(new ArrayList<>());
            columns.forEach(c -> {
                if(tableInfo.getTableName().equals(c.getTableName())){
                    tableInfo.getColumns().add(c);
                }
            });
            // 主键
            tableInfo.setPrimaryKeys(this.getPrimaryKeys(tableInfo.getTableSchem(), tableInfo.getTableName()));
            // 索引信息
            tableInfo.setIndexInfo(this.getIndexInfo2(tableInfo.getTableSchem(), tableInfo.getTableName()));
            // 其他表引用本表的键（外键）
            tableInfo.setExportedKeys(this.getExportedKeys(tableInfo.getTableSchem(), tableInfo.getTableName()));
            // 本表引用其他表的键（外键）
            tableInfo.setImportedKeys(this.getImportedKeys(tableInfo.getTableSchem(), tableInfo.getTableName()));
            // from jdbc end...

            // 约束
            tableInfo.setConstraintInfo(new ArrayList<>());
            constraintInfo.forEach(ci -> {
                if(tableInfo.getTableName().equals(ci.getTableName())){
                    this.formatConstraintColumnName(ci, tableInfo.getColumns());
                    tableInfo.getConstraintInfo().add(ci);
                }
            });
        });

        return tables;
    }

    /**
     * 获取表的约束信息
     * @param tableName 表名
     * @return List<ConstraintInfo>
     */
    public List<ConstraintInfo> getConstraintInfo(String[] tableName) {
        return this.getDatabaseDao().getConstraintInfo(tableName);
    }

    /**
     * 获取本表所有外键
     * @param schema 模式
     * @param tableName 表名
     * @return List<ImportedKey>
     */
    public List<ImportedKey> getImportedKeys(String schema, String tableName) {
        return this.getDatabaseDao().getImportedKeys(schema, tableName);
    }

    /**
     * 获取本表被引用的键（外键）
     * @param schema 模式
     * @param tableName 表名
     * @return List<ExportedKey>
     */
    public List<ExportedKey> getExportedKeys(String schema, String tableName) {
        return this.getDatabaseDao().getExportedKeys(schema, tableName);
    }

    /**
     * 获取索引信息
     * @param schema 模式
     * @param tableName 表名
     * @return List<IndexInfo2>
     */
    public List<IndexInfo2> getIndexInfo2(String schema, String tableName){
        return this.getDatabaseDao().getIndexInfo(schema, tableName);
    }

    /**
     * 获取所有主键
     * @param schema 模式
     * @param tableName 表名
     * @return List<PrimaryKey>
     */
    public List<PrimaryKey> getPrimaryKeys(String schema, String tableName) {
        return this.getDatabaseDao().getPrimaryKeys(schema, tableName);
    }

    /**
     * 获取所有列
     * @param tableName 表名
     * @return List<ColumnInfo>
     */
    public List<ColumnInfo> getColumns(String... tableName) {
        return this.getDatabaseDao().getColumns(tableName);
    }

    /**
     * 获取所有表
     * @param tableName 表名
     * @return List<TableInfo>
     */
    public List<TableInfo> getTables(String... tableName) {
        return this.getDatabaseDao().getTables(tableName);
    }


    /**
     * 格式化检查约束中的字段的名称
     * @param ci 约束条件
     * @param columnInfoList 所有列
     */
    public void formatConstraintColumnName(ConstraintInfo ci, List<ColumnInfo> columnInfoList) {
        String checkCondition = ci.getCheckCondition();
        // 去掉特有的格式
        for(ColumnInfo columnInfo: columnInfoList){
            checkCondition = checkCondition.replaceAll(
                    StringUtils.escape(this.getRightName(columnInfo.getColumnName())),
                    columnInfo.getColumnName());
        }
        ci.setCheckCondition(checkCondition);
    }

    @Override
    public void createAllTable(List<TableInfo> tis) {
        for(TableInfo ti: tis){
            try{
                this.createTable(ti);
                this.migrateData(ti);
            } catch (Exception e){
                // java.sql.BatchUpdateException: General error
                e.printStackTrace();
                convertedFailTables.add(ti.getTableName());
                log2("\033[31m" + e.getMessage() + "\033[m");
            }
        }
    }

    @Override
    public void createTable(TableInfo ti) {
        boolean isNextAction = this.beforeCreateTable(ti);
        if(!isNextAction){
            return;
        }
        StringBuilder sqlBuilder = this.buildCreateTable(ti);
        log(sqlBuilder.toString());

        this.getDatabaseDao().getJdbcTemplate().execute(sqlBuilder.toString());

        log2("Table {} created.", this.getRightName(ti.getTableName()));

        this.afterCreateTable(ti);
    }


    /**
     * 构造约束信息
     * @param ti 表信息
     * @param sqlBuilder Builder
     */
    public void buildConstraintInfo(TableInfo ti, StringBuilder sqlBuilder) {
        if(ti.getConstraintInfo().size() == 0){
            return;
        }

        // 约束
        ti.getConstraintInfo().forEach(constraintInfo -> {
            sqlBuilder.append("CONSTRAINT ").append(getRightName(constraintInfo.getConstraintName())).append(" ");
            if(constraintInfo.getConstraintType().equals(ConstraintInfo.TYPE_CHECK_CONSTRAINT)){
                sqlBuilder.append("CHECK ");
            }
            sqlBuilder.append(constraintInfo.getCheckCondition()).append(",");
            // if(this.isSameDatabaseVendor()){
            //     // 相同的数据库厂家
            //     sqlBuilder.append(constraintInfo.getCheckCondition()).append(",");
            // }
        });
    }

    /**
     * 构造主键信息
     * 原则上，一个表只有一个主键，主键可以包含一列或多列
     * 将List<PrimaryKey>转成字符串， 如：PRIMARY KEY (col1, col2, col3, ...)
     * @param ti 表信息
     * @param sqlBuilder Builder
     */
    public void buildPrimaryKeys(TableInfo ti, StringBuilder sqlBuilder) {
        if(ti.getPrimaryKeys().size() == 0){
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
     * @param ti 表信息
     */
    public void buildForeignKeys(TableInfo ti) {

        if (ti == null || ti.getImportedKeys() == null) {
            return;
        }

        StringBuilder sBuilder = new StringBuilder();
        for (ImportedKey importedKey : ti.getImportedKeys()) {

            sBuilder.append("ALTER TABLE ").append(this.getRightName(ti.getTableName())).append(" ");
            sBuilder.append("ADD CONSTRAINT ").append(importedKey.getFkName()).append(" ");
            sBuilder.append("FOREIGN KEY (").append(this.getRightName(importedKey.getFkColumnName())).append(") ");
            sBuilder.append("REFERENCES ").append(this.getRightName(importedKey.getPkTableName())).append("(")
                    .append(this.getRightName(importedKey.getPkColumnName())).append(") ");

            // 更新规则
            buildForeignKeyChangeRules(sBuilder, importedKey);

            this.getDatabaseDao().insertMetadata(new SlaveMetaDataEntity(ti.getTableName(), SlaveMetaDataEntity.TYPE_FOREIGN_KEY, sBuilder.toString()));

            // 清空缓冲区
            sBuilder.delete(0, sBuilder.length());
        }
    }

    /**
     * 更新规则，如果数据库不支持的话，需要重写。
     * @param sBuilder 语句
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
     * @param ti 表信息
     */
    public abstract void buildIndex(TableInfo ti);

    /**
     * 构造表注释、列注释
     * @param ti 表信息
     */
    protected void buildComment(TableInfo ti) {

    }

    /**
     * 处理字符串、数字、日期的长度、精度问题
     */
    protected abstract void handleDataType(StringBuilder sqlBuilder, ColumnInfo columnInfo);


    @Override
    public int getCharBytes() {
        return this.getDatabaseDao().getDatabaseInfo().getMaxLen();
    }

    @Override
    public void pullData(TableInfo ti, List<DatabaseConverter> slaveDatabaseConverters) {

    }

    @Override
    public void migrateData(TableInfo ti) {

        // 迁移表的数据
        // 查询原系统的数据
        DatabaseConverter masterDatabaseConverter = ConfigManager.getMasterDatabaseConverter();
        int pageSize = masterDatabaseConverter.getDatabaseConfig().getPageSize();

        BasicDatabaseDao masterDatabaseDao = masterDatabaseConverter.getDatabaseDao();
        // 获取表的行数
        long tableRowCount = masterDatabaseDao.getTableRowCount(ti.getTableSchem(), ti.getTableName());
        if(tableRowCount == 0) {
            // 无数据
            return;
        }

        // 截断表
        if(this.getDatabaseConfig().isTruncateTable()){
            this.getDatabaseDao().truncateTable(this.getRightName(ti.getTableName()));
        }

        // 生成sql语句：
        // insert into T (col1, col2, col3, ...) values (?, ?, ?, ...)
        final StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ").append(this.getRightName(ti.getTableName())).append("(");
        final StringBuilder sqlPlaceHolderBuilder = new StringBuilder();

        int i = 0;
        int[] argTypes = new int[ti.getColumns().size()];
        for(ColumnInfo c : ti.getColumns()){
            sqlBuilder.append(this.getRightName(c.getColumnName())).append(",");
            sqlPlaceHolderBuilder.append("?,");
            argTypes[i++] = this.changeDataType(c.getDataType());
        }

        sqlBuilder.delete(sqlBuilder.length() - 1, sqlBuilder.length()).append(") VALUES (");
        sqlPlaceHolderBuilder.delete(sqlPlaceHolderBuilder.length() - 1, sqlPlaceHolderBuilder.length());
        sqlBuilder.append(sqlPlaceHolderBuilder).append(")");

        // 插入缓冲区
        String sql;

        // 不分页
        if(tableRowCount <= pageSize){
            sql = String.format("SELECT * FROM %s", masterDatabaseConverter.getRightName(ti.getTableName()));
            this.batchUpdate(masterDatabaseDao, sqlBuilder, sql, argTypes);
            return;
        }

        // 分页插入
        // 计算表的页数
        long tablePageSize = (tableRowCount / pageSize) + (tableRowCount % pageSize > 0 ? 1 : 0);
        for(i = 1; i < tablePageSize; i++){
            // 循环获取每一个分页的数据
            final int offset = (i - 1) * pageSize;
            sql = masterDatabaseConverter.getPaginationSQL(ti, offset, pageSize);
            this.batchUpdate(masterDatabaseDao, sqlBuilder, sql, argTypes);
        }

    }



    /**
     * 批量插入
     * @param masterDatabaseDao BasicDatabaseDao
     * @param sqlBuilder insert sql语句，如：insert into T (col1, col2, col3, ...) values (?, ?, ?, ...)
     * @param querySql 查询执行的SQL
     */
    private void batchUpdate(BasicDatabaseDao masterDatabaseDao,
                             StringBuilder sqlBuilder,
                             String querySql,
                             int[] argTypes) {

        // 源端
        final JdbcTemplate2 masterJdbcTemplate = masterDatabaseDao.getJdbcTemplate();
        // 目标端
        final JdbcTemplate2 jdbcTemplate = this.getDatabaseDao().getJdbcTemplate();

        // 查询某一页的所有的数据
        List<Object[]> batchArgs = masterJdbcTemplate.query(querySql, (rs, rowNum) -> {
            Object[] values = new Object[rs.getMetaData().getColumnCount()];
            for (int i = 0, len = values.length; i < len; i++) {
                values[i] = rs.getObject(i + 1);
            }
            return values;
        });

        // sqlserver:
        // 将截断字符串或二进制数据
        // https://support.microsoft.com/en-us/topic/kb4468101-improvement-optional-replacement-for-string-or-binary-data-would-be-truncated-message-with-extended-information-in-sql-server-2016-and-2017-a4279ad6-1d3b-3960-77ef-c82a909f4b89

        // 批量插入
        try{
            // jdbcTemplate.batchUpdate(sqlBuilder.toString(), new BatchPreparedStatementSetter() {
            //     @Override
            //     public void setValues(@NonNull PreparedStatement ps, int i) throws SQLException {
            //         Object[] values = batchArgs.get(i);
            //         int colIndex = 0;
            //         for (Object value : values) {
            //             colIndex++;
            //             // StatementCreatorUtils.setParameterValue(ps, colIndex, SqlTypeValue.TYPE_UNKNOWN, value);
            //             if(value == null){
            //                 ps.setNull(colIndex, argTypes[colIndex - 1]);
            //                 continue;
            //             }
            //
            //             if(argTypes[colIndex - 1] == Types.CLOB){
            //                 ps.setClob(colIndex, new StringReader(value.toString()));
            //             } else {
            //                 ps.setObject(colIndex, value, argTypes[colIndex - 1]);
            //             }
            //         }
            //     }
            //     @Override
            //     public int getBatchSize() {
            //         return batchArgs.size();
            //     }
            // });
            jdbcTemplate.batchUpdate(sqlBuilder.toString(), batchArgs, argTypes);

        } catch (Throwable e){
            log2("\033[33m" + e.getMessage().replace(sqlBuilder.toString(), "...") + "\033[m");
        }

    }

    /**
     * 修改列的类型，针对不支持的可以动态修改
     * @param dataType 数据类型
     * @return 数据类型
     */
    protected int changeDataType(int dataType) {
        return dataType;
    }


    @Override
    public void applySlaveDatabaseMetadata() {
        // 应用元数据信息
        List<SlaveMetaDataEntity> list = this.getDatabaseDao().getSlaveMetaDatas(SlaveMetaDataEntity.NOT_USED);
        for(SlaveMetaDataEntity entity : list){
            // 执行语句
            this.getDatabaseDao().getJdbcTemplate().execute(entity.getStatement());
            // 更新状态，是否执行过
            entity.setIsUsed(SlaveMetaDataEntity.USED);
            this.getDatabaseDao().updateMetadata(entity);
        }
    }
}
