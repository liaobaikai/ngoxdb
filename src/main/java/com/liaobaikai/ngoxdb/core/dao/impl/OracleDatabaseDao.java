package com.liaobaikai.ngoxdb.core.dao.impl;

import com.liaobaikai.ngoxdb.bean.NgoxDbMaster;
import com.liaobaikai.ngoxdb.bean.info.ColumnInfo;
import com.liaobaikai.ngoxdb.bean.info.ConstraintInfo;
import com.liaobaikai.ngoxdb.bean.info.DatabaseInfo;
import com.liaobaikai.ngoxdb.bean.info.IndexInfo2;
import com.liaobaikai.ngoxdb.core.dao.BasicDatabaseDao;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import oracle.jdbc.OracleConnection;
import org.slf4j.Logger;
import org.springframework.jdbc.core.ConnectionCallback;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Oracle数据库访问
 *
 * @author baikai.liao
 * @Time 2021-01-31 23:33:15
 */
@Slf4j
public class OracleDatabaseDao extends BasicDatabaseDao {

    @Getter
    enum Statement {

        // 查询数据库支持的字符集
        // QUERY_SUPPORT_CHARSET("SELECT UNIQUE VALUE FROM V$NLS_VALID_VALUES WHERE PARAMETER = ?"),

        // 查询数据库字符集信息
        // QUERY_DATABASE_CHARSET("SELECT " +
        //         "NULL AS TABLE_CAT, " +
        //         "USER AS TABLE_SCHEM, " +
        //         "USERENV('LANGUAGE') CHARSET_NAME, " +
        //         "LENGTHB('\uD842\uDFB7') MAX_LEN " +
        //         "FROM DUAL"),

        // QUERY_ALL_TABLE("SELECT " +
        //         "NULL TABLE_CAT, " +
        //         "T.OWNER TABLE_SCHEM, " +
        //         "T.TABLE_NAME, " +
        //         "TC.TABLE_TYPE," +
        //         "TC.COMMENTS REMARKS " +
        //         "FROM ALL_TABLES T LEFT JOIN ALL_TAB_COMMENTS TC " +
        //         "ON T.OWNER = TC.OWNER AND T.TABLE_NAME = TC.TABLE_NAME " +
        //         "WHERE T.OWNER = ? "),
        //
        // QUERY_TABLE(QUERY_ALL_TABLE.getStatement() + "AND T.TABLE_NAME IN(?)"),

        QUERY_ALL_CHECK_CONSTRAINT("SELECT " +
                "NULL AS TABLE_CAT, " +
                "OWNER AS TABLE_SCHEM, " +
                "TABLE_NAME, " +
                "CONSTRAINT_NAME, " +
                "SEARCH_CONDITION AS CHECK_CONDITION, " +
                "CONSTRAINT_TYPE " +
                "FROM ALL_CONSTRAINTS WHERE OWNER = ? AND CONSTRAINT_TYPE = 'C'"),

        QUERY_CHECK_CONSTRAINT(QUERY_ALL_CHECK_CONSTRAINT.getStatement() + " AND TABLE_NAME IN (?)"),


        // 查询表的列的其他信息
        // QUERY_ALL_COLUMN_EXTEND("SELECT " +
        //         " COLUMN_NAME, " +
        //         " COLUMN_TYPE, " +
        //         " CHARACTER_SET_NAME AS CHARSET_NAME, " +
        //         " COLLATION_NAME, " +
        //         " COLUMN_DEFAULT AS COLUMN_DEF, " +
        //         " EXTRA " +
        //         " FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ?"),
        // QUERY_COLUMN_EXTEND(QUERY_ALL_COLUMN_EXTEND.statement + "AND TABLE_NAME in(?)"),

        ;

        private final String statement;

        Statement(String statement) {
            this.statement = statement;
        }

    }

    private final DatabaseInfo databaseInfo;

    public OracleDatabaseDao(NgoxDbMaster ngoxDbMaster) {
        super(ngoxDbMaster);

        // oracle jdbc源码：
        // oracle.jdbc.OracleDatabaseMetaData.getTables()
        // oracle.jdbc.driver.OracleDatabaseMetaData.getColumns()
        this.getJdbcTemplate().execute((ConnectionCallback<Void>) con -> {
            if (con.isWrapperFor(OracleConnection.class)) {
                OracleConnection oConnection = con.unwrap(OracleConnection.class);
                oConnection.setRemarksReporting(true);
            }
            return null;
        });

        this.databaseInfo = this.initDatabaseInfo();
    }

    @Override
    protected DatabaseInfo initDatabaseInfo() {
        // 数据库默认字符集
        // 数据库支持的字符集
        // https://docs.oracle.com/goldengate/1212/gg-winux/GWUAD/wu_charsets.htm#GWUAD733
        // return this.getJdbcTemplate()
        //         .queryForList(Statement.QUERY_DATABASE_CHARSET.statement, DatabaseInfo.class).get(0);
        return null;
    }

    @Override
    public Logger getLogger() {
        return log;
    }

    @Override
    public String getSchema() {
        return this.getNgoxDbMaster().getDatabaseConfig().getUsername().toUpperCase();
    }

    @Override
    public DatabaseInfo getDatabaseInfo() {
        return this.databaseInfo;
    }

    // @Override
    // public List<TableInfo> getTables(String... tableName) {
    //     // oracle.jdbc.OracleDatabaseMetaData.getTables()
    //
    //     final StringBuilder sqlBuilder = new StringBuilder(
    //             (tableName.length == 0 ? OracleStatement.QUERY_ALL_TABLE : OracleStatement.QUERY_TABLE).getStatement());
    //     Object[] params = this.getParams(sqlBuilder, tableName, null, this.getSchemaPattern());
    //
    //     return this.getJdbcTemplate().queryForList(sqlBuilder.toString(), TableInfo.class, params);
    // }
    //
    @Override
    public List<ColumnInfo> getColumns(String... tableName) {
        // oracle.jdbc.driver.OracleDatabaseMetaData.getColumns()

        List<ColumnInfo> columns = super.getColumns(tableName);
        columns.forEach(columnInfo -> {
            // 默认值的问题
            // 对于nvarchar，nchar类型的默认值
            switch (columnInfo.getDataType()) {
                case Types.NCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                case Types.NCLOB:
                    // 默认值的格式：u'defaultvalue'
                    if (columnInfo.getColumnDef() != null
                            && columnInfo.getColumnDef().length() > 0
                            && columnInfo.getColumnDef().charAt(0) == 'u') {
                        columnInfo.setColumnDef(columnInfo.getColumnDef().substring(1));
                    }
                    break;
                default:
                    // oracle支持
            }
        });

        return columns;
    }

    @Override
    public List<ConstraintInfo> getConstraintInfo(String... tableName) {
        StringBuilder sqlBuilder = new StringBuilder(
                (tableName.length > 0 ? Statement.QUERY_CHECK_CONSTRAINT : Statement.QUERY_ALL_CHECK_CONSTRAINT).getStatement());

        Object[] params = this.getParams(sqlBuilder, tableName, null, this.getSchema());

        final List<ConstraintInfo> resultList = this.getJdbcTemplate().queryForList(sqlBuilder.toString(), ConstraintInfo.class, params);

        // 1.名称问题
        // 2.过滤非空约束，如"USER_NAME" IS NOT NULL
        List<ConstraintInfo> finalResultList = new ArrayList<>();
        resultList.forEach(ci -> {
            if (!ci.getCheckCondition().endsWith("IS NOT NULL")) {
                ci.setCheckCondition(String.format("(%s)", ci.getCheckCondition()));
                finalResultList.add(ci);
            }
        });

        return finalResultList;
    }

    @Override
    public List<IndexInfo2> getIndexInfo(String tableName) {
        return this.getIndexInfo(null, this.getSchema(), tableName, false, false);
    }

    private List<IndexInfo2> getIndexInfo(String catalog, String schema, String table,
                                          boolean unique, boolean approximate) {

        final String string = "select null as table_cat,\n       owner as table_schem,\n       table_name,\n       0 as NON_UNIQUE,\n       null as index_qualifier,\n       null as index_name, 0 as type,\n       0 as ordinal_position, null as column_name,\n       null as asc_or_desc,\n       num_rows as cardinality,\n       blocks as pages,\n       null as filter_condition\nfrom all_tables\nwhere table_name = ?\n";
        String string2 = "";
        if (schema != null && schema.length() > 0) {
            string2 = "  and owner = ?\n";
        }
        final String string3 = "select null as table_cat,\n       i.owner as table_schem,\n       i.table_name,\n       decode (i.uniqueness, 'UNIQUE', 0, 1),\n       null as index_qualifier,\n       i.index_name,\n       1 as type,\n       c.column_position as ordinal_position,\n       c.column_name,\n       null as asc_or_desc,\n       i.distinct_keys as cardinality,\n       i.leaf_blocks as pages,\n       null as filter_condition\nfrom all_indexes i, all_ind_columns c\nwhere i.table_name = ?\n";
        String string4 = "";
        if (schema != null && schema.length() > 0) {
            string4 = "  and i.owner = ?\n";
        }
        String s4 = "";
        if (unique) {
            s4 = "  and i.uniqueness = 'UNIQUE'\n";
        }
        final String string5 = string + string2 + "union\n" + string3 + string4 + s4 + "  and i.index_name = c.index_name\n  and i.table_owner = c.table_owner\n  and i.table_name = c.table_name\n  and i.owner = c.index_owner\n" + "order by non_unique, type, index_name, ordinal_position\n";

        return this.getJdbcTemplate().queryForList(string5, IndexInfo2.class, table, schema, table, schema);
    }

}
