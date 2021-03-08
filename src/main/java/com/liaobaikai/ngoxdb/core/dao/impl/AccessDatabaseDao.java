package com.liaobaikai.ngoxdb.core.dao.impl;

import com.alibaba.fastjson.JSON;
import com.liaobaikai.ngoxdb.bean.info.ColumnInfo;
import com.liaobaikai.ngoxdb.bean.info.ConstraintInfo;
import com.liaobaikai.ngoxdb.bean.info.DatabaseInfo;
import com.liaobaikai.ngoxdb.bean.info.IndexInfo2;
import com.liaobaikai.ngoxdb.bean.rs.ExportedKey;
import com.liaobaikai.ngoxdb.bean.rs.ImportedKey;
import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
import com.liaobaikai.ngoxdb.core.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.core.udf.AccessDatabaseFunctions;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.ucanaccess.jdbc.UcanaccessConnection;
import oracle.jdbc.OracleConnection;
import org.slf4j.Logger;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.support.JdbcUtils;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Access数据库访问
 * http://ucanaccess.sourceforge.net/site.html
 *
 *
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
                "FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS"),
        ;

        private final String statement;

        Statement(String statement) {
            this.statement = statement;
        }
    }

    private final DatabaseInfo databaseInfo;

    public AccessDatabaseDao(JdbcTemplate2 jdbcTemplate) {
        super(jdbcTemplate);
        this.databaseInfo = initDatabaseInfo();
        // this.addCustomFunctions();
    }

    /**
     * 添加自定义函数
     */
    private void addCustomFunctions(){
        this.getJdbcTemplate().execute((ConnectionCallback<Void>) con -> {
            if (con.isWrapperFor(UcanaccessConnection.class)) {

                UcanaccessConnection ucanaccessConnection = con.unwrap(UcanaccessConnection.class);
                ucanaccessConnection.addFunctions(AccessDatabaseFunctions.class);
            }
            return null;
        });
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
                case Types.VARCHAR:
                    // 长度大于255的话，就是nclob类型

                    if (column.getColumnSize() <= 255) {
                        column.setSourceDataType(Types.VARCHAR);

                        column.setDataType(Types.NVARCHAR);
                        column.setSqlDataType(Types.NVARCHAR);
                        column.setTypeName("NVARCHAR");
                    } else {
                        column.setSourceDataType(Types.CLOB);

                        column.setDataType(Types.NCLOB);
                        column.setSqlDataType(Types.NCLOB);
                        column.setTypeName("NCLOB");
                    }
                    break;
                case Types.CHAR:
                    // char 改成 nchar
                    column.setSourceDataType(Types.CHAR);

                    column.setDataType(Types.NCHAR);
                    column.setSqlDataType(Types.NCHAR);
                    column.setTypeName("NCHAR");
                    break;
                case Types.CLOB:
                    // clob 改成 nclob
                    column.setSourceDataType(Types.CLOB);

                    column.setDataType(Types.NCLOB);
                    column.setSqlDataType(Types.NCLOB);
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

    @Override
    public List<ConstraintInfo> getConstraintInfo(String... tableName) {
        List<ConstraintInfo> list = this.getJdbcTemplate().queryForList2(Statement.QUERY_ALL_CHECK_CONSTRAINT.getStatement(), ConstraintInfo.class);
        list.removeIf(item -> {
            String[] splitValues = item.getCheckCondition().split("\\.");
            if (tableName.length > 0) {
                for (String tName : tableName) {
                    if (!tName.equals(splitValues[1])) {
                        return true;
                    }
                }
            }

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

    /**
     * 创建表
     *
     * @param tableName 表名
     */
    @Override
    public void createMetadataTable(String tableName) {
        final String sqlBuilder = "CREATE TABLE " + tableName + "( " +
                "metadata_id    varchar(" + UUID.randomUUID().toString().length() + ") unique," +
                "table_name     varchar(128)," +
                "type           varchar(32)," +
                "statement      varchar(250)," +
                "is_used        int" +
                ")";

        this.getJdbcTemplate().execute(sqlBuilder);
    }

    @Override
    public void truncateTable(String tableName) {
        // Truncate command is not exists!!
        // Using delete command replaced.
        // After delete, using file space is not recovery.
        // this.deleteTable(tableName);
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
        try{
            super.deleteTable(tableName);
        }catch (Exception e){
            if(e.getMessage().contains("integrity constraint violation: foreign key no action")){
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
