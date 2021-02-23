package com.liaobaikai.ngoxdb.dao.impl;

import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
import com.liaobaikai.ngoxdb.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.info.ColumnInfo;
import com.liaobaikai.ngoxdb.info.ConstraintInfo;
import com.liaobaikai.ngoxdb.info.DatabaseInfo;
import com.liaobaikai.ngoxdb.info.IndexInfo2;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.List;
import java.util.UUID;

/**
 * Access数据库访问
 * https://docs.microsoft.com/zh-cn/office/vba/api/overview/access
 * http://ucanaccess.sourceforge.net/site.html
 * https://www.runoob.com/sql/sql-datatypes.html
 *
 * http://www.hsqldb.org/doc/2.0/guide/databaseobjects-chapt.html
 *
 * Access Offline 帮助文档(数据库自带)
 *
 * HSQLDB
 * select table_catalog, table_schema, table_name from information_schema.tables:
 *
 * PUBLIC	INFORMATION_SCHEMA	ADMINISTRABLE_ROLE_AUTHORIZATIONS
 * PUBLIC	INFORMATION_SCHEMA	APPLICABLE_ROLES
 * PUBLIC	INFORMATION_SCHEMA	ASSERTIONS
 * PUBLIC	INFORMATION_SCHEMA	AUTHORIZATIONS
 * PUBLIC	INFORMATION_SCHEMA	CHARACTER_SETS
 * PUBLIC	INFORMATION_SCHEMA	CHECK_CONSTRAINTS
 * PUBLIC	INFORMATION_SCHEMA	CHECK_CONSTRAINT_ROUTINE_USAGE
 * PUBLIC	INFORMATION_SCHEMA	COLLATIONS
 * PUBLIC	INFORMATION_SCHEMA	COLUMNS
 * PUBLIC	INFORMATION_SCHEMA	COLUMN_COLUMN_USAGE
 * PUBLIC	INFORMATION_SCHEMA	COLUMN_DOMAIN_USAGE
 * PUBLIC	INFORMATION_SCHEMA	COLUMN_PRIVILEGES
 * PUBLIC	INFORMATION_SCHEMA	COLUMN_UDT_USAGE
 * PUBLIC	INFORMATION_SCHEMA	CONSTRAINT_COLUMN_USAGE
 * PUBLIC	INFORMATION_SCHEMA	CONSTRAINT_PERIOD_USAGE
 * PUBLIC	INFORMATION_SCHEMA	CONSTRAINT_TABLE_USAGE
 * PUBLIC	INFORMATION_SCHEMA	DATA_TYPE_PRIVILEGES
 * PUBLIC	INFORMATION_SCHEMA	DOMAINS
 * PUBLIC	INFORMATION_SCHEMA	DOMAIN_CONSTRAINTS
 * PUBLIC	INFORMATION_SCHEMA	ELEMENT_TYPES
 * PUBLIC	INFORMATION_SCHEMA	ENABLED_ROLES
 * PUBLIC	INFORMATION_SCHEMA	INFORMATION_SCHEMA_CATALOG_NAME
 * PUBLIC	INFORMATION_SCHEMA	JARS
 * PUBLIC	INFORMATION_SCHEMA	JAR_JAR_USAGE
 * PUBLIC	INFORMATION_SCHEMA	KEY_COLUMN_USAGE
 * PUBLIC	INFORMATION_SCHEMA	KEY_PERIOD_USAGE
 * PUBLIC	INFORMATION_SCHEMA	PARAMETERS
 * PUBLIC	INFORMATION_SCHEMA	PERIODS
 * PUBLIC	INFORMATION_SCHEMA	REFERENTIAL_CONSTRAINTS
 * PUBLIC	INFORMATION_SCHEMA	ROLE_AUTHORIZATION_DESCRIPTORS
 * PUBLIC	INFORMATION_SCHEMA	ROLE_COLUMN_GRANTS
 * PUBLIC	INFORMATION_SCHEMA	ROLE_ROUTINE_GRANTS
 * PUBLIC	INFORMATION_SCHEMA	ROLE_TABLE_GRANTS
 * PUBLIC	INFORMATION_SCHEMA	ROLE_UDT_GRANTS
 * PUBLIC	INFORMATION_SCHEMA	ROLE_USAGE_GRANTS
 * PUBLIC	INFORMATION_SCHEMA	ROUTINES
 * PUBLIC	INFORMATION_SCHEMA	ROUTINE_COLUMN_USAGE
 * PUBLIC	INFORMATION_SCHEMA	ROUTINE_JAR_USAGE
 * PUBLIC	INFORMATION_SCHEMA	ROUTINE_PERIOD_USAGE
 * PUBLIC	INFORMATION_SCHEMA	ROUTINE_PRIVILEGES
 * PUBLIC	INFORMATION_SCHEMA	ROUTINE_ROUTINE_USAGE
 * PUBLIC	INFORMATION_SCHEMA	ROUTINE_SEQUENCE_USAGE
 * PUBLIC	INFORMATION_SCHEMA	ROUTINE_TABLE_USAGE
 * PUBLIC	INFORMATION_SCHEMA	SCHEMATA
 * PUBLIC	INFORMATION_SCHEMA	SEQUENCES
 * PUBLIC	INFORMATION_SCHEMA	SQL_FEATURES
 * PUBLIC	INFORMATION_SCHEMA	SQL_IMPLEMENTATION_INFO
 * PUBLIC	INFORMATION_SCHEMA	SQL_PACKAGES
 * PUBLIC	INFORMATION_SCHEMA	SQL_PARTS
 * PUBLIC	INFORMATION_SCHEMA	SQL_SIZING
 * PUBLIC	INFORMATION_SCHEMA	SQL_SIZING_PROFILES
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_BESTROWIDENTIFIER
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_CACHEINFO
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_COLUMNS
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_COLUMN_SEQUENCE_USAGE
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_COMMENTS
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_CONNECTION_PROPERTIES
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_CROSSREFERENCE
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_INDEXINFO
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_INDEXSTATS
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_KEY_INDEX_USAGE
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_PRIMARYKEYS
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_PROCEDURECOLUMNS
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_PROCEDURES
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_PROPERTIES
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_SCHEMAS
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_SEQUENCES
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_SESSIONINFO
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_SESSIONS
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_SYNONYMS
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_TABLES
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_TABLESTATS
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_TABLETYPES
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_TEXTTABLES
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_TYPEINFO
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_UDTS
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_USERS
 * PUBLIC	INFORMATION_SCHEMA	SYSTEM_VERSIONCOLUMNS
 * PUBLIC	INFORMATION_SCHEMA	TABLES
 * PUBLIC	INFORMATION_SCHEMA	TABLE_CONSTRAINTS
 * PUBLIC	INFORMATION_SCHEMA	TABLE_PRIVILEGES
 * PUBLIC	INFORMATION_SCHEMA	TRANSLATIONS
 * PUBLIC	INFORMATION_SCHEMA	TRIGGERED_UPDATE_COLUMNS
 * PUBLIC	INFORMATION_SCHEMA	TRIGGERS
 * PUBLIC	INFORMATION_SCHEMA	TRIGGER_COLUMN_USAGE
 * PUBLIC	INFORMATION_SCHEMA	TRIGGER_PERIOD_USAGE
 * PUBLIC	INFORMATION_SCHEMA	TRIGGER_ROUTINE_USAGE
 * PUBLIC	INFORMATION_SCHEMA	TRIGGER_SEQUENCE_USAGE
 * PUBLIC	INFORMATION_SCHEMA	TRIGGER_TABLE_USAGE
 * PUBLIC	INFORMATION_SCHEMA	UDT_PRIVILEGES
 * PUBLIC	INFORMATION_SCHEMA	USAGE_PRIVILEGES
 * PUBLIC	INFORMATION_SCHEMA	USER_DEFINED_TYPES
 * PUBLIC	INFORMATION_SCHEMA	VIEWS
 * PUBLIC	INFORMATION_SCHEMA	VIEW_COLUMN_USAGE
 * PUBLIC	INFORMATION_SCHEMA	VIEW_PERIOD_USAGE
 * PUBLIC	INFORMATION_SCHEMA	VIEW_ROUTINE_USAGE
 * PUBLIC	INFORMATION_SCHEMA	VIEW_TABLE_USAGE
 * PUBLIC	SYSTEM_LOBS	BLOCKS
 * PUBLIC	SYSTEM_LOBS	LOBS
 * PUBLIC	SYSTEM_LOBS	LOB_IDS
 * PUBLIC	SYSTEM_LOBS	PARTS
 *
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
            if(column.getColumnDef() != null){
                // 转换为最终的默认值效果
                column.setColumnDef(this.getFinalDefaultValue(column.getColumnDef(), column.getDataType()));
            }
        });
        return columns;
    }

    @Override
    public void removePrimaryIndex(List<IndexInfo2> indexInfoList) {
        if(indexInfoList == null){
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
            if(tableName.length > 0){
                for(String tName: tableName){
                    if(!tName.equals(splitValues[1])){
                        return true;
                    }
                }
            }

            // 排除非空的检查约束
            if(splitValues[2].endsWith("IS NOT NULL")){
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
     * @param tableName 表名
     */
    @Override
    public void createMetadataTable(String tableName){
        final String sqlBuilder = "CREATE TABLE " + tableName + "( " +
                "metadata_id    varchar("+ UUID.randomUUID().toString().length() +") unique," +
                "table_name     varchar(128)," +
                "type           varchar(32)," +
                "statement      varchar(250)," +
                "is_used        int" +
                ")";

        this.getJdbcTemplate().execute(sqlBuilder);
    }

}
