package com.liaobaikai.ngoxdb.core.converter;

import com.liaobaikai.ngoxdb.bean.ColumnType;
import com.liaobaikai.ngoxdb.bean.NgoxDbMaster;
import com.liaobaikai.ngoxdb.bean.info.ColumnInfo;
import com.liaobaikai.ngoxdb.bean.info.ConstraintInfo;
import com.liaobaikai.ngoxdb.bean.info.IndexInfo2;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.bean.rs.ImportedKey;
import com.liaobaikai.ngoxdb.bean.rs.PrimaryKey;
import com.liaobaikai.ngoxdb.core.config.ConfigManager;
import com.liaobaikai.ngoxdb.core.config.DatabaseConfig;
import com.liaobaikai.ngoxdb.core.dialect.DatabaseDialect;
import com.liaobaikai.ngoxdb.core.entity.NgoxDbRelayLog;
import com.liaobaikai.ngoxdb.utils.StringUtils;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author baikai.liao
 * @Time 2021-03-18 15:49:04
 */
public abstract class TableCreator extends TableCollector {

    public abstract boolean isSameDatabaseVendor();

    public abstract void handleException(Throwable e);

    public abstract DatabaseConfig getDatabaseConfig();

    public abstract DatabaseDialect getDatabaseDialect();

    public abstract NgoxDbMaster getNgoxDbMaster();

    public abstract Logger getLogger();

    public abstract List<String> getCreateFailTables();

    public abstract List<String> getCreatedTables();

    public abstract List<String> getSkipCreateTables();

    public abstract Map<String, String> getRemapTable();

    public abstract Map<String, String> getRemapColumn();

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // build
    ////////////////////////////////////////////////////////////////////////////////////////////////

    protected String getFinalTableName(String name) {
        String remap = this.getRemapTable().get(name);
        return remap == null ? name : remap;
    }

    protected String getFinalColumnName(String finalTableName, String name){
        String remap = this.getRemapColumn().get(String.format("%s.%s", finalTableName, name));
        return remap == null ? name : remap.substring(finalTableName.length() + 1);
    }

    /**
     * 创建一个表之前的操作
     *
     * @param ti 表信息
     */
    public String buildCreateTable(TableInfo ti) {

        StringBuilder sBuilder = new StringBuilder("create table ");

        String finalTableName = getFinalTableName(ti.getTableName());

        sBuilder.append(this.toLookupName(finalTableName)).append(" (");

        // 列信息
        ti.getColumns().forEach(columnInfo -> {

            // 列名
            String finalColumnName = getFinalColumnName(finalTableName, columnInfo.getColumnName());

            sBuilder.append(this.toLookupName(finalColumnName)).append(" ");

            // 支持自动标识符替换列类型
            // 相同数据库无需再次解析
            ColumnType columnType = columnInfo.getMapOfColumnType().get(this.getDatabaseConfig().getDatabase());
            if (columnType == null) {
                columnType = getDatabaseDialect().getColumnType(columnInfo);
                columnType.setColumnName(finalColumnName);
                columnInfo.getMapOfColumnType().put(this.getDatabaseConfig().getDatabase(), columnType);
            }

            // 是否支持自动增长类型替换掉列类型（access）
            if (getDatabaseDialect().supportsIdentityReplaceColumnType() && columnInfo.isAutoincrement()) {
                sBuilder.append(getIdentityColumnString(columnType));
            } else {
                sBuilder.append(columnType);
            }

            // 排序规则
            if (StringUtils.isNotEmpty(columnInfo.getCollationName()) && getDatabaseDialect().supportsCollationOnBuildTable()) {
                sBuilder.append(" ").append(getDatabaseDialect().getCollationString(columnInfo.getCollationName())).append(" ");
            }

            // 默认值
            if (columnInfo.getColumnDef() != null) {
                // 判断默认值是否为函数
                String dataTypeDef = ConfigManager.getDatabaseDataTypeDefault(columnInfo.getColumnDef(),
                        getNgoxDbMaster().getMasterDatabaseVendor(),
                        this.getDatabaseConfig().getDatabase());

                // 转成boolean类型
                dataTypeDef = getDatabaseDialect().toBooleanValueString(dataTypeDef == null ? columnInfo.getColumnDef() : dataTypeDef, columnType.getJdbcDataType());
                // 其他类型
                sBuilder.append(" default ").append(dataTypeDef).append(" ");
            }

            if (columnInfo.isNotNull()) {
                sBuilder.append(" not null ");
            }

            // 扩展信息，仅支持同数据库厂家
            if (StringUtils.isNotEmpty(columnInfo.getExtra()) && this.isSameDatabaseVendor()) {
                sBuilder.append(" ").append(columnInfo.getExtra()).append(" ");
            }

            // 自动增长，且不支持自动增长替换列类型
            if (getDatabaseDialect().supportsIdentityColumns()
                    && !getDatabaseDialect().supportsIdentityReplaceColumnType()
                    && columnInfo.isAutoincrement()) {
                // 是否支持自动增长必须为主键
                if (getDatabaseDialect().doesIdentityMustPrimaryKey() && ti.getPrimaryKeys().size() == 0) {
                    sBuilder.append(" primary key ");
                }
                sBuilder.append(getDatabaseDialect().getIdentityColumnString()).append(" ");
            }

            // 注释
            if (getDatabaseDialect().supportsCommentOnBuildTable() && StringUtils.isNotEmpty(columnInfo.getRemarks())) {
                sBuilder.append(" ");
                sBuilder.append(
                        getDatabaseDialect().getColumnCommentString(
                                getFinalTableName(ti.getTableName()),
                                finalColumnName,
                                columnInfo.getRemarks()
                        ));
            }
            sBuilder.append(",");

        });

        // 主键
        buildPrimaryKeys(ti, sBuilder);
        // 外键约束
        buildCheckConstraint(ti, sBuilder);

        sBuilder.delete(sBuilder.length() - 1, sBuilder.length()).append(")");

        getLogger().info("[{}] table: {}, create table statement.", this.getDatabaseConfig().getName(),
                getDatabaseDialect().toLookupName(getFinalTableName(ti.getTableName())));

        return sBuilder.toString();
    }

    protected String getIdentityColumnString(ColumnType columnType) {
        return getDatabaseDialect().getIdentityColumnString();
    }

    /**
     * 构造约束信息
     *
     * @param ti         表信息
     * @param sqlBuilder Builder
     */
    public void buildCheckConstraint(TableInfo ti, StringBuilder sqlBuilder) {
        if (ti.getConstraintInfo().size() == 0) {
            return;
        }

        // 检查约束
        ti.getConstraintInfo().forEach(constraintInfo -> {
            sqlBuilder.append(" constraint ").append(this.toLookupName(constraintInfo.getConstraintName())).append(" ");
            if (constraintInfo.getConstraintType().equalsIgnoreCase(ConstraintInfo.TYPE_CHECK_CONSTRAINT)) {
                sqlBuilder.append("check ");
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

        sqlBuilder.append(" primary key (");

        String finalTableName = this.getFinalTableName(ti.getTableName());

        // 再次排序
        ti.getPrimaryKeys().sort(Comparator.comparing(PrimaryKey::getKeySeq));
        ti.getPrimaryKeys().forEach(pk -> sqlBuilder.append(this.toLookupName(getFinalColumnName(finalTableName, pk.getColumnName()))).append(","));
        sqlBuilder.delete(sqlBuilder.length() - 1, sqlBuilder.length()).append("),");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // create table
    ////////////////////////////////////////////////////////////////////////////////////////////////

    public void beforeCreateAll(List<TableInfo> tis) {

        String tName = NgoxDbRelayLog.TABLE_NAME;
        if (getDatabaseDao().existsTable(tName)) {
            // 支持删除表
            if (getDatabaseDialect().supportsDropTable()) {
                // 清空表信息
                getDatabaseDao().dropLogTable();
                // 表不存在，先创建
                getDatabaseDao().createLogTable();
            } else {
                // 不支持删除表
                getLogger().info("[{}] skip create table {}, already exists.", this.getDatabaseConfig().getName(),
                        getDatabaseDialect().toLookupName(tName));
            }
        } else {
            // 表不存在，先创建
            getDatabaseDao().createLogTable();
        }

        // 先删除表，然后再重新创建
        if (getDatabaseDialect().supportsDropTable() && this.getDatabaseConfig().isReplaceTable()) {

            // 统一删除外键
            for (TableInfo ti : tis) {
                getDatabaseDao().dropForeignKey(getFinalTableName(ti.getTableName()));
            }

            // 删除表
            this.getParallelMaster().parallelExecute(tis.size(), (index) -> {
                String finalTableName = getFinalTableName(tis.get(index).getTableName());
                getDatabaseDao().dropTable(finalTableName);
            });

        }
    }

    public void createAll(List<TableInfo> tis) {

        // 重置日志表
        try {
            this.beforeCreateAll(tis);

            // 多线程创建表
            this.getParallelMaster().parallelExecute(tis.size(), (index) -> {
                TableInfo tableInfo = tis.get(index);
                this.create(tableInfo);
            });

        } catch (Exception e) {
            this.handleException(e);
        }
    }

    /**
     * 创建表之前的操作
     *
     * @param ti 表信息
     * @return 是否继续创建表
     */
    public boolean beforeCreateTable(TableInfo ti) {
        // 查询表是否存在
        String finalTableName = getFinalTableName(ti.getTableName());
        // 表存在的话，需要跳过
        if (!this.getDatabaseConfig().isReplaceTable() && getDatabaseDao().existsTable(finalTableName)) {
            getLogger().warn("[{}] table {} already exists, skip.", this.getDatabaseConfig().getName(), this.toLookupName(finalTableName));
            return false;
        }
        return true;
    }

    /**
     * 创建单个表
     *
     * @param ti 表信息
     */
    public void create(TableInfo ti) {

        String finalTableName = getFinalTableName(ti.getTableName());
        String tipTableName = finalTableName + "(" + ti.getTableName() + ")";

        // 是否需要继续创建表
        boolean isNextAction = this.beforeCreateTable(ti);
        if (!isNextAction) {
            getSkipCreateTables().add(tipTableName);
            return;
        }

        // 构建创建表的ddl语句
        final String ddl = this.buildCreateTable(ti);
        getLogger().info("[{}] {}", this.getDatabaseConfig().getName(), ddl);

        NgoxDbRelayLog ngoxDbRelayLog = new NgoxDbRelayLog(finalTableName, NgoxDbRelayLog.TYPE_CREATE_TABLE, ddl);

        // 执行创建表
        try {
            getDatabaseDao().execute(ddl);
            ngoxDbRelayLog.setLogUsed(1);
        } catch (Exception e) {
            this.handleException(e);
            getCreateFailTables().add(tipTableName);
            return;
        }

        getDatabaseDao().insertLogRows(ngoxDbRelayLog);

        // 创建表后的回调
        this.afterCreateTable(ti);

        // 刚创建的表
        getCreatedTables().add(finalTableName);

    }

    /**
     * 创建一个表之后的操作
     *
     * @param ti 表信息
     */
    public void afterCreateTable(TableInfo ti) {

        String finalTableName = getFinalTableName(ti.getTableName());
        String escapeTableName = getDatabaseDialect().toLookupName(finalTableName);

        getLogger().info("[{}] build foreign keys on table {}.", this.getDatabaseConfig().getName(), escapeTableName);

        // 外键
        this.buildForeignKeys(ti);

        getLogger().info("[{}] build indexes on table {}.", this.getDatabaseConfig().getName(), escapeTableName);

        // 索引 & 唯一键
        this.buildGenericIndex(ti);

        // 注释
        if (getDatabaseDialect().supportsCommentOn()) {
            getLogger().info("[{}] build table and column comments on table {}.", this.getDatabaseConfig().getName(), escapeTableName);
            this.buildComment(ti);
        }

        this.bindColumnType(finalTableName, ti);

    }

    /**
     * 从数据库中绑定对应的类型
     *
     * @param tableName 表名
     * @param ti        表信息
     */
    protected void bindColumnType(String tableName, TableInfo ti) {

        // 获取目标数据库的类型
        List<ColumnInfo> cis = this.getDatabaseDao().getColumns(getFinalTableName(tableName));
        // 数据库厂家
        String database = this.getDatabaseConfig().getDatabase();
        for (ColumnInfo srcColumnInfo : ti.getColumns()) {
            for (ColumnInfo targetColumnInfo : cis) {
                if (targetColumnInfo.getColumnName().equalsIgnoreCase(srcColumnInfo.getColumnName())) {
                    // 列名相同
                    ColumnType columnType = new ColumnType();
                    columnType.setJdbcDataType(targetColumnInfo.getDataType());
                    columnType.setColumnName(targetColumnInfo.getColumnName());
                    columnType.setTypeName(targetColumnInfo.getTypeName());
                    columnType.setUnsigned(targetColumnInfo.isUnsigned());
                    columnType.setFromDb(true);
                    srcColumnInfo.getMapOfColumnType().put(database, columnType);
                    break;
                }
            }
        }

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
        List<String> usedFkNames = new ArrayList<>();
        for (ImportedKey ik : ti.getImportedKeys()) {

            String constraintName = ik.getFkName();

            if (usedFkNames.contains(constraintName)) {
                continue;
            }
            usedFkNames.add(constraintName);

            List<String> pkColumnNames = new ArrayList<>();
            List<String> fkColumnNames = new ArrayList<>();

            sBuilder.append("alter table ").append(this.toLookupName(getFinalTableName(ik.getFkTableName()))).append(" ");
            for (ImportedKey ik2 : ti.getImportedKeys()) {
                if (!constraintName.equals(ik2.getFkName())) {
                    continue;
                }
                fkColumnNames.add(ik2.getFkColumnName());
                pkColumnNames.add(ik2.getPkColumnName());
            }

            // add constraint foreign key (...) references ..(...);
            sBuilder.append(getDatabaseDialect().getAddForeignKeyConstraintString(this.buildFKName(getFinalTableName(ik.getFkTableName())),
                    StringUtils.toArray(fkColumnNames), getFinalTableName(ik.getPkTableName()), StringUtils.toArray(pkColumnNames)));

            // 更新规则
            buildForeignKeyChangeRules(sBuilder, ik);

            getDatabaseDao().insertLogRows(new NgoxDbRelayLog(getFinalTableName(ti.getTableName()), NgoxDbRelayLog.TYPE_FOREIGN_KEY, sBuilder.toString()));

            // 清空缓冲区
            sBuilder.delete(0, sBuilder.length());
        }
    }

    protected String buildFKName(String finalTableName) {
        // 格式：fk_tableName_rndstr length<=28
        StringBuilder stringBuilder = new StringBuilder("fk_");
        stringBuilder.append(finalTableName).append("_");
        if(stringBuilder.length() > 28){
            stringBuilder.delete(3, stringBuilder.length());
        }
        stringBuilder.append(UUID.randomUUID().toString().replaceAll("-", ""), 0, 28 - stringBuilder.length());
        return stringBuilder.toString();
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
    public void buildGenericIndex(TableInfo ti) {
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

        // 一个键名，多个列
        StringBuilder sBuilder = new StringBuilder();
        List<String> buildIndex = new ArrayList<>();
        String indexType;

        for (IndexInfo2 ii : ti.getIndexInfo()) {

            if (ii.isTableIndexStatistic() || buildIndex.contains(ii.getIndexName())) {
                // 统计信息不需要。
                continue;
            }
            buildIndex.add(ii.getIndexName());

            sBuilder.append("create");
            if (!ii.isNonUnique()) {
                indexType = NgoxDbRelayLog.TYPE_UNIQUE_INDEX;
                sBuilder.append(" unique ");
            } else {

                switch (ii.getType()) {
                    case IndexInfo2.tableIndexClustered:
                        if (getDatabaseDialect().supportsClusteredIndex()) {
                            // https://docs.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql?view=sql-server-ver15
                            sBuilder.append(" clustered ");
                        }
                        break;
                    case IndexInfo2.tableIndexFullText:
                        // 全文索引
                        if (getDatabaseDialect().supportsFullTextIndex()) {
                            sBuilder.append(" ").append(getDatabaseDialect().getFullTextIndexString()).append(" ");
                        }
                        break;
                    case IndexInfo2.tableIndexSpatial:
                        // 空间索引
                        if (getDatabaseDialect().supportsSpatialIndex()) {
                            sBuilder.append(" spatial ");
                        }
                        break;
                    case IndexInfo2.tableIndexBitmap:
                        // 位图索引
                        if (getDatabaseDialect().supportsBitmapIndex()) {
                            sBuilder.append(" bitmap ");
                        }
                        break;
                }

                indexType = NgoxDbRelayLog.TYPE_INDEX;
            }

            sBuilder.append(" index ")
                    .append(this.buildIndexName(ii, ti))
                    .append(" on ").append(this.toLookupName(getFinalTableName(ii.getTableName())));

            if (this.buildIndexExtensionClause(sBuilder, ii, ti)) {
                if (sBuilder.length() > 0) {
                    getDatabaseDao().insertLogRows(new NgoxDbRelayLog(getFinalTableName(ti.getTableName()), indexType, sBuilder.toString()));
                    // 全部清掉
                    sBuilder.delete(0, sBuilder.length());
                }
                continue;
            }

            sBuilder.append(" ( ");

            // 获取所有的columnName
            for (IndexInfo2 ii2 : ti.getIndexInfo()) {
                if (!ii.getIndexName().equals(ii2.getIndexName())) {
                    continue;
                }
                sBuilder.append(this.toLookupName(ii2.getColumnName())).append(" ").append(ii2.getOrder()).append(", ");
            }
            sBuilder.delete(sBuilder.length() - 2, sBuilder.length()).append(" )");

            // 构建索引后的操作
            this.afterBuildIndex(sBuilder, ii, ti);

            if (sBuilder.length() > 0) {
                getDatabaseDao().insertLogRows(new NgoxDbRelayLog(getFinalTableName(ti.getTableName()), indexType, sBuilder.toString()));
                // 全部清掉
                sBuilder.delete(0, sBuilder.length());
            }

        }
    }

    /**
     * 构建索引的时候，是否需要扩展的子句
     *
     * @param sBuilder buffer
     * @param ii       索引信息
     * @param ti       表信息
     * @return true: 使用, false不使用
     */
    protected boolean buildIndexExtensionClause(StringBuilder sBuilder, IndexInfo2 ii, TableInfo ti) {
        return false;
    }

    /**
     * 创建索引后的操作
     *
     * @param sBuilder buffer
     * @param ii       索引信息
     * @param ti       表信息
     */
    protected void afterBuildIndex(StringBuilder sBuilder, IndexInfo2 ii, TableInfo ti) {
    }

    /**
     * 索引名称
     *
     * @param ii 索引
     * @param ti 表信息
     * @return 索引名称
     */
    protected String buildIndexName(IndexInfo2 ii, TableInfo ti) {

        if (!this.getDatabaseConfig().isGenerateName()) {
            return this.toLookupName(ii.getIndexName());
        }

        int maxIdentifierLength = getDatabaseDao().getMaxIdentifierLength();
        if (maxIdentifierLength == 0) {
            maxIdentifierLength = 64;
        }

        StringBuilder sBuilder = new StringBuilder(maxIdentifierLength);

        if (ii.isNonUnique()) {
            sBuilder.append("idx_");
        } else {
            sBuilder.append("ux_");
        }
        sBuilder.append(getFinalTableName(ti.getTableName()).toLowerCase()).append("_");

        StringBuilder columnBuilder = new StringBuilder();
        // 获取所有的columnName
        for (IndexInfo2 ii2 : ti.getIndexInfo()) {
            if (!ii.getIndexName().equals(ii2.getIndexName())) {
                continue;
            }
            columnBuilder.append(ii2.getColumnName().toLowerCase()).append("_");
        }
        columnBuilder.delete(columnBuilder.length() - 1, columnBuilder.length());

        if ((sBuilder.length() + columnBuilder.length()) > maxIdentifierLength) {
            // 组装所有列的时候，超过长度限制。
            sBuilder.append(UUID.randomUUID().toString().replaceAll("-", ""), 0, maxIdentifierLength - sBuilder.length());
        } else {
            sBuilder.append(columnBuilder);
            // sBuilder.append("-").append(UUID.randomUUID().toString().replaceAll("-", ""), 0, 5);
        }

        return this.toLookupName(sBuilder.toString());
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
        // DM database
        //
        // SQLServer database
        //
        if (!getDatabaseDialect().supportsCommentOn()) {
            return;
        }

        final String finalTableName = getFinalTableName(ti.getTableName());

        // 列注释
        ti.getColumns().forEach(c -> {
            if (StringUtils.isEmpty(c.getRemarks())) {
                return;
            }
            String s = getDatabaseDialect().getColumnCommentString(finalTableName, c.getColumnName(), c.getRemarks());
            getDatabaseDao().insertLogRows(new NgoxDbRelayLog(finalTableName, NgoxDbRelayLog.TYPE_COMMENT, s));

        });

        // 表注释
        if (StringUtils.isNotEmpty(ti.getRemarks())) {
            String s = getDatabaseDialect().getTableCommentString(finalTableName, ti.getRemarks());
            getDatabaseDao().insertLogRows(new NgoxDbRelayLog(finalTableName, NgoxDbRelayLog.TYPE_COMMENT, s));
        }
    }

}
