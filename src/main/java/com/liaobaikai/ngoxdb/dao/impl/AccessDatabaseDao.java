package com.liaobaikai.ngoxdb.dao.impl;

import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
import com.liaobaikai.ngoxdb.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.info.ColumnInfo;
import com.liaobaikai.ngoxdb.info.ConstraintInfo;
import com.liaobaikai.ngoxdb.info.DatabaseInfo;
import com.liaobaikai.ngoxdb.info.IndexInfo2;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.hsqldb.types.Types;

import java.sql.DatabaseMetaData;
import java.util.List;
import java.util.UUID;

/**
 * Access数据库访问
 * http://ucanaccess.sourceforge.net/site.html
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
            // 更新数据类型
            switch (column.getDataType()){
                case Types.VARCHAR:
                    // 长度大于255的话，就是clob类型
                    column.setSourceDataType(Types.VARCHAR);
                    if(column.getColumnSize() <= 255){
                        column.setDataType(Types.NVARCHAR);
                        column.setSqlDataType(Types.NVARCHAR);
                        column.setTypeName("NVARCHAR");
                    } else {
                        column.setDataType(Types.CLOB);
                        column.setSqlDataType(Types.CLOB);
                        column.setTypeName("CLOB");
                    }
                    break;
                case Types.CHAR:
                    // clob 改成 nclob
                    column.setSourceDataType(Types.CHAR);
                    column.setDataType(Types.NCHAR);
                    column.setTypeName("NCHAR");
                    break;
                case Types.CLOB:
                    column.setSourceDataType(Types.CLOB);
                    column.setDataType(Types.NCLOB);
                    column.setTypeName("NCLOB");
                    break;
            }
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

    @Override
    public void truncateTable(String tableName) {
        // 删除后，不收缩空间。占用的存储会越来越大。
        this.deleteTable(tableName);
    }
}
