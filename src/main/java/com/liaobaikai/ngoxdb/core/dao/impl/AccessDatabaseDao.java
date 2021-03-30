package com.liaobaikai.ngoxdb.core.dao.impl;

import com.liaobaikai.ngoxdb.bean.NgoxDbMaster;
import com.liaobaikai.ngoxdb.bean.info.ColumnInfo;
import com.liaobaikai.ngoxdb.bean.info.ConstraintInfo;
import com.liaobaikai.ngoxdb.bean.info.DatabaseInfo;
import com.liaobaikai.ngoxdb.bean.info.IndexInfo2;
import com.liaobaikai.ngoxdb.core.constant.JdbcDataType;
import com.liaobaikai.ngoxdb.core.dao.BasicDatabaseDao;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;

/**
 * Access数据库访问
 * http://ucanaccess.sourceforge.net/site.html
 * <p>
 * <p>
 * INFORMATION_SCHEMA: ADMINISTRABLE_ROLE_AUTHORIZATIONS
 * INFORMATION_SCHEMA: APPLICABLE_ROLES
 * INFORMATION_SCHEMA: ASSERTIONS
 * INFORMATION_SCHEMA: AUTHORIZATIONS
 * INFORMATION_SCHEMA: CHARACTER_SETS
 * INFORMATION_SCHEMA: CHECK_CONSTRAINTS
 * INFORMATION_SCHEMA: CHECK_CONSTRAINT_ROUTINE_USAGE
 * INFORMATION_SCHEMA: COLLATIONS
 * INFORMATION_SCHEMA: COLUMNS
 * INFORMATION_SCHEMA: COLUMN_COLUMN_USAGE
 * INFORMATION_SCHEMA: COLUMN_DOMAIN_USAGE
 * INFORMATION_SCHEMA: COLUMN_PRIVILEGES
 * INFORMATION_SCHEMA: COLUMN_UDT_USAGE
 * INFORMATION_SCHEMA: CONSTRAINT_COLUMN_USAGE
 * INFORMATION_SCHEMA: CONSTRAINT_PERIOD_USAGE
 * INFORMATION_SCHEMA: CONSTRAINT_TABLE_USAGE
 * INFORMATION_SCHEMA: DATA_TYPE_PRIVILEGES
 * INFORMATION_SCHEMA: DOMAINS
 * INFORMATION_SCHEMA: DOMAIN_CONSTRAINTS
 * INFORMATION_SCHEMA: ELEMENT_TYPES
 * INFORMATION_SCHEMA: ENABLED_ROLES
 * INFORMATION_SCHEMA: INFORMATION_SCHEMA_CATALOG_NAME
 * INFORMATION_SCHEMA: JARS
 * INFORMATION_SCHEMA: JAR_JAR_USAGE
 * INFORMATION_SCHEMA: KEY_COLUMN_USAGE
 * INFORMATION_SCHEMA: KEY_PERIOD_USAGE
 * INFORMATION_SCHEMA: PARAMETERS
 * INFORMATION_SCHEMA: PERIODS
 * INFORMATION_SCHEMA: REFERENTIAL_CONSTRAINTS
 * INFORMATION_SCHEMA: ROLE_AUTHORIZATION_DESCRIPTORS
 * INFORMATION_SCHEMA: ROLE_COLUMN_GRANTS
 * INFORMATION_SCHEMA: ROLE_ROUTINE_GRANTS
 * INFORMATION_SCHEMA: ROLE_TABLE_GRANTS
 * INFORMATION_SCHEMA: ROLE_UDT_GRANTS
 * INFORMATION_SCHEMA: ROLE_USAGE_GRANTS
 * INFORMATION_SCHEMA: ROUTINES
 * INFORMATION_SCHEMA: ROUTINE_COLUMN_USAGE
 * INFORMATION_SCHEMA: ROUTINE_JAR_USAGE
 * INFORMATION_SCHEMA: ROUTINE_PERIOD_USAGE
 * INFORMATION_SCHEMA: ROUTINE_PRIVILEGES
 * INFORMATION_SCHEMA: ROUTINE_ROUTINE_USAGE
 * INFORMATION_SCHEMA: ROUTINE_SEQUENCE_USAGE
 * INFORMATION_SCHEMA: ROUTINE_TABLE_USAGE
 * INFORMATION_SCHEMA: SCHEMATA
 * INFORMATION_SCHEMA: SEQUENCES
 * INFORMATION_SCHEMA: SQL_FEATURES
 * INFORMATION_SCHEMA: SQL_IMPLEMENTATION_INFO
 * INFORMATION_SCHEMA: SQL_PACKAGES
 * INFORMATION_SCHEMA: SQL_PARTS
 * INFORMATION_SCHEMA: SQL_SIZING
 * INFORMATION_SCHEMA: SQL_SIZING_PROFILES
 * INFORMATION_SCHEMA: SYSTEM_BESTROWIDENTIFIER
 * INFORMATION_SCHEMA: SYSTEM_CACHEINFO
 * INFORMATION_SCHEMA: SYSTEM_COLUMNS
 * INFORMATION_SCHEMA: SYSTEM_COLUMN_SEQUENCE_USAGE
 * INFORMATION_SCHEMA: SYSTEM_COMMENTS
 * INFORMATION_SCHEMA: SYSTEM_CONNECTION_PROPERTIES
 * INFORMATION_SCHEMA: SYSTEM_CROSSREFERENCE
 * INFORMATION_SCHEMA: SYSTEM_INDEXINFO
 * INFORMATION_SCHEMA: SYSTEM_INDEXSTATS
 * INFORMATION_SCHEMA: SYSTEM_KEY_INDEX_USAGE
 * INFORMATION_SCHEMA: SYSTEM_PRIMARYKEYS
 * INFORMATION_SCHEMA: SYSTEM_PROCEDURECOLUMNS
 * INFORMATION_SCHEMA: SYSTEM_PROCEDURES
 * INFORMATION_SCHEMA: SYSTEM_PROPERTIES
 * INFORMATION_SCHEMA: SYSTEM_SCHEMAS
 * INFORMATION_SCHEMA: SYSTEM_SEQUENCES
 * INFORMATION_SCHEMA: SYSTEM_SESSIONINFO
 * INFORMATION_SCHEMA: SYSTEM_SESSIONS
 * INFORMATION_SCHEMA: SYSTEM_SYNONYMS
 * INFORMATION_SCHEMA: SYSTEM_TABLES
 * INFORMATION_SCHEMA: SYSTEM_TABLESTATS
 * INFORMATION_SCHEMA: SYSTEM_TABLETYPES
 * INFORMATION_SCHEMA: SYSTEM_TEXTTABLES
 * INFORMATION_SCHEMA: SYSTEM_TYPEINFO
 * INFORMATION_SCHEMA: SYSTEM_UDTS
 * INFORMATION_SCHEMA: SYSTEM_USERS
 * INFORMATION_SCHEMA: SYSTEM_VERSIONCOLUMNS
 * INFORMATION_SCHEMA: TABLES
 * INFORMATION_SCHEMA: TABLE_CONSTRAINTS
 * INFORMATION_SCHEMA: TABLE_PRIVILEGES
 * INFORMATION_SCHEMA: TRANSLATIONS
 * INFORMATION_SCHEMA: TRIGGERED_UPDATE_COLUMNS
 * INFORMATION_SCHEMA: TRIGGERS
 * INFORMATION_SCHEMA: TRIGGER_COLUMN_USAGE
 * INFORMATION_SCHEMA: TRIGGER_PERIOD_USAGE
 * INFORMATION_SCHEMA: TRIGGER_ROUTINE_USAGE
 * INFORMATION_SCHEMA: TRIGGER_SEQUENCE_USAGE
 * INFORMATION_SCHEMA: TRIGGER_TABLE_USAGE
 * INFORMATION_SCHEMA: UDT_PRIVILEGES
 * INFORMATION_SCHEMA: USAGE_PRIVILEGES
 * INFORMATION_SCHEMA: USER_DEFINED_TYPES
 * INFORMATION_SCHEMA: VIEWS
 * INFORMATION_SCHEMA: VIEW_COLUMN_USAGE
 * INFORMATION_SCHEMA: VIEW_PERIOD_USAGE
 * INFORMATION_SCHEMA: VIEW_ROUTINE_USAGE
 * INFORMATION_SCHEMA: VIEW_TABLE_USAGE
 * SYS: MSYSACCESSSTORAGE
 * SYS: MSYSACES
 * SYS: MSYSCOMPLEXCOLUMNS
 * SYS: MSYSCOMPLEXTYPE_ATTACHMENT
 * SYS: MSYSCOMPLEXTYPE_DECIMAL
 * SYS: MSYSCOMPLEXTYPE_GUID
 * SYS: MSYSCOMPLEXTYPE_IEEEDOUBLE
 * SYS: MSYSCOMPLEXTYPE_IEEESINGLE
 * SYS: MSYSCOMPLEXTYPE_LONG
 * SYS: MSYSCOMPLEXTYPE_SHORT
 * SYS: MSYSCOMPLEXTYPE_TEXT
 * SYS: MSYSCOMPLEXTYPE_UNSIGNEDBYTE
 * SYS: MSYSNAVPANEGROUPCATEGORIES
 * SYS: MSYSNAVPANEGROUPS
 * SYS: MSYSNAVPANEGROUPTOOBJECTS
 * SYS: MSYSNAVPANEOBJECTIDS
 * SYS: MSYSOBJECTS
 * SYS: MSYSQUERIES
 * SYS: MSYSRELATIONSHIPS
 * SYSTEM_LOBS: BLOCKS
 * SYSTEM_LOBS: LOBS
 * SYSTEM_LOBS: LOB_IDS
 * SYSTEM_LOBS: PARTS
 * UCA_METADATA: COLUMNS
 * UCA_METADATA: COLUMNS_VIEW
 * UCA_METADATA: PROP
 * UCA_METADATA: TABLES
 *
 * @author baikai.liao
 * @Time 2021-01-27 16:24:08
 */
@Slf4j
public class AccessDatabaseDao extends BasicDatabaseDao {

    @Getter
    enum Statement {
        QUERY_ALL_CHECK_CONSTRAINT("SELECT " +
                "CONSTRAINT_CATALOG AS TABLE_CAT, " +
                "CONSTRAINT_SCHEMA AS TABLE_SCHEM, " +
                "CONSTRAINT_NAME, " +
                "CHECK_CLAUSE AS CHECK_CONDITION, " +
                "'C' as CONSTRAINT_TYPE " +
                "FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS WHERE CHECK_CLAUSE LIKE ?"),
        ;

        private final String statement;

        Statement(String statement) {
            this.statement = statement;
        }
    }

    private final DatabaseInfo databaseInfo;

    public AccessDatabaseDao(NgoxDbMaster ngoxDbMaster) {
        super(ngoxDbMaster);
        this.databaseInfo = initDatabaseInfo();
    }

    @Override
    protected DatabaseInfo initDatabaseInfo() {
        DatabaseInfo databaseInfo = new DatabaseInfo();
        databaseInfo.setTableCat("PUBLIC");
        databaseInfo.setTableSchem("PUBLIC");
        databaseInfo.setMaxLen(2);
        databaseInfo.setCharsetName("GBK");
        return databaseInfo;
    }

    @Override
    public Logger getLogger() {
        return log;
    }

    @Override
    public String getSchema() {
        return this.databaseInfo.getTableSchem();
    }

    @Override
    public DatabaseInfo getDatabaseInfo() {
        return this.databaseInfo;
    }

    @Override
    public String getCreateViewDefinition(String viewName) {
        log.error("Microsoft-Access database not support query create view definition!!!!");
        return null;
    }

    @Override
    public List<ColumnInfo> getColumns(String... tableName) {
        List<ColumnInfo> columns = super.getColumns(tableName);
        columns.forEach(column -> {
            // 更新数据类型
            switch (column.getDataType()) {
                case JdbcDataType.VARCHAR:
                    // 长度大于255的话，就是nclob类型
                    int maximumPrecision = this.getDatabaseDialect().getMaximumPrecision(column.getDataType());
                    if (column.getColumnSize() <= maximumPrecision) {
                        column.setSourceDataType(JdbcDataType.VARCHAR);

                        column.setDataType(JdbcDataType.NVARCHAR);
                        column.setSqlDataType(JdbcDataType.NVARCHAR);
                        column.setTypeName("NVARCHAR");
                    } else {
                        column.setSourceDataType(JdbcDataType.CLOB);

                        column.setDataType(JdbcDataType.NCLOB);
                        column.setSqlDataType(JdbcDataType.NCLOB);
                        column.setTypeName("NCLOB");
                    }
                    break;
                case JdbcDataType.CHAR:
                    // char 改成 nchar
                    column.setSourceDataType(JdbcDataType.CHAR);

                    column.setDataType(JdbcDataType.NCHAR);
                    column.setSqlDataType(JdbcDataType.NCHAR);
                    column.setTypeName("NCHAR");
                    break;
                case JdbcDataType.CLOB:
                    // clob 改成 nclob
                    column.setSourceDataType(JdbcDataType.CLOB);

                    column.setDataType(JdbcDataType.NCLOB);
                    column.setSqlDataType(JdbcDataType.NCLOB);
                    column.setTypeName("NCLOB");
                    break;
            }
            if (column.getColumnDef() != null) {
                // 转换为最终的默认值效果
                column.setColumnDef(this.getFinalDefaultValue(column.getColumnDef(), column.getDataType()));
            }
        });
        return columns;
    }

    @Override
    public void removePrimaryIndex(List<IndexInfo2> indexInfoList) {
        if (indexInfoList == null) {
            return;
        }
        // 主键默认就是索引，因此需要删除主键的索引信息。在创建表的同时不需要再次创建一个唯一的索引。
        indexInfoList.removeIf(ii -> ii.getType() != DatabaseMetaData.tableIndexStatistic && ii.getIndexName().toUpperCase().startsWith("SYS_IDX_SYS_PK"));
    }

    /**
     * 获取检查约束
     *
     * @param tableName 表名：schemaName.tableName
     * @return 检查约束列表
     */
    private List<ConstraintInfo> getCheckConstraintInfo(String tableName) {

        List<ConstraintInfo> list =
                this.getJdbcTemplate().queryForList(Statement.QUERY_ALL_CHECK_CONSTRAINT.getStatement(),
                        ConstraintInfo.class, tableName);

        list.removeIf(item -> {
            String[] splitValues = item.getCheckCondition().split("\\.");

            // 排除非空的检查约束
            if (splitValues[2].endsWith("IS NOT NULL")) {
                return true;
            }

            item.setCheckCondition(splitValues[2]);
            item.setTableName(splitValues[1]);

            return false;
        });
        return list;
    }

    @Override
    public List<ConstraintInfo> getConstraintInfo(String... tableNames) {

        // PUBLIC.COUNTRY.
        List<ConstraintInfo> finalList = new ArrayList<>();

        if (tableNames.length > 0) {
            for (String tableName : tableNames) {
                finalList.addAll(this.getCheckConstraintInfo(String.format("%s.%s%%", this.getSchema(), tableName.toUpperCase())));
            }
        } else {
            finalList.addAll(this.getCheckConstraintInfo(String.format("%s.%%", this.getSchema())));
        }

        return finalList;
    }

    // @Override
    // public List<ExportedKey> getExportedKeys(String schema, String tableName) {
    //     // select * from information_schema.TABLE_CONSTRAINTS
    //     // "CONSTRAINT_CATALOG" -> "PUBLIC"
    //     // "CONSTRAINT_SCHEMA" -> "PUBLIC"
    //     // "CONSTRAINT_NAME" -> "ADDRESS_FK_ADDRESS_CITY"
    //     // "CONSTRAINT_TYPE" -> "FOREIGN KEY"
    //     // "TABLE_CATALOG" -> "PUBLIC"
    //     // "TABLE_SCHEMA" -> "PUBLIC"
    //     // "TABLE_NAME" -> "ADDRESS"
    //     // "IS_DEFERRABLE" -> "NO"
    //     // "INITIALLY_DEFERRED" -> "NO"
    //     return new ArrayList<>();
    // }


    @Override
    public void deleteTable(String tableName) {
        try {
            super.deleteTable(tableName);
        } catch (Exception e) {
            if (e.getMessage().contains("integrity constraint violation: foreign key no action")) {
                // 忽略
            } else {
                this.getLogger().error("", e);
            }
        }

    }

    @Override
    public void dropTable(String tableName) {

    }

    // /**
    //  * 级联删除表
    //  * @param tableName 表名
    //  */
    // private void dropTableCascade(String tableName){
    //
    //     List<ExportedKey> exportedKeys = this.getExportedKeys(this.getCatalog(), tableName);
    //
    //     if(exportedKeys.size() == 0){
    //         // 直接删除
    //         this.dropTable2("[" + tableName + "]");
    //     } else {
    //         for(ExportedKey exportedKey: exportedKeys){
    //             this.dropTableCascade(exportedKey.getFkTableName());
    //         }
    //     }
    // }

    // /**
    //  * 正式删除表动作
    //  * @param tableName 表名
    //  */
    // private void dropTable2(String tableName){
    //     this.getJdbcTemplate().execute((ConnectionCallback<Void>) con -> {
    //         con.setAutoCommit(false);
    //         String ddl = String.format("drop table %s", tableName);
    //         getLogger().info("[{}] {}", this.getJdbcTemplate().getDatabaseConfig().getName(), ddl);
    //         PreparedStatement preparedStatement = con.prepareStatement(ddl);
    //         boolean result = preparedStatement.execute();
    //         con.commit();
    //         getLogger().info("[{}] result={}", this.getJdbcTemplate().getDatabaseConfig().getName(), result);
    //         JdbcUtils.closeStatement(preparedStatement);
    //         DataSourceUtils.releaseConnection(con, this.getJdbcTemplate().getDataSource());
    //         return null;
    //     });
    // }
}
