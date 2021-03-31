package com.liaobaikai.ngoxdb.core.dao.impl;

import com.liaobaikai.ngoxdb.bean.NgoxDbMaster;
import com.liaobaikai.ngoxdb.bean.info.ColumnInfo;
import com.liaobaikai.ngoxdb.bean.info.ConstraintInfo;
import com.liaobaikai.ngoxdb.bean.info.DatabaseInfo;
import com.liaobaikai.ngoxdb.core.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.utils.StringUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Pg database数据访问
 *
 * @author baikai.liao
 * @Time 2021-02-01 15:53:47
 */
@Slf4j
public class PgDatabaseDao extends BasicDatabaseDao {

    @Getter
    enum Statement {

        // 查询数据库支持的字符集
        // QUERY_SUPPORT_CHARSET("select unique value from v$nls_valid_values where parameter = ?"),

        // 查询数据库字符集信息
        // QUERY_DATABASE_CHARSET("select " +
        //         "datname table_cat, " +
        //         "null as table_schem, " +
        //         "pg_encoding_to_char(encoding) as charset_name " +
        //         "from pg_database where datname = ?"),

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

        QUERY_ALL_CHECK_CONSTRAINT("select " +
                "null as table_cat, " +
                "n.nspname as table_schem," +
                "cs.relname as table_name," +
                "c.conname as constraint_name, " +
                "pg_get_constraintdef(c.oid, true) as check_condition, " +
                "c.contype as constraint_type " +
                "from pg_constraint c " +
                "join pg_class cs on c.conrelid = cs.oid " +
                "join pg_namespace n on n.oid = cs.relnamespace " +
                "where c.contype = 'c'"),

        QUERY_CHECK_CONSTRAINT(QUERY_ALL_CHECK_CONSTRAINT.getStatement() + " and cs.relname in (?)"),

        ;

        private final String statement;

        Statement(String statement) {
            this.statement = statement;
        }

    }

    // https://www.postgresql.org/docs/8.1/datatype.html
    final static String[] dataTypes = {
            "bigint",
            "bigserial",
            "bit varying",
            "bit",
            "boolean",
            "box",
            "bytea",
            "character varying",
            "character",
            "bpchar",
            "cidr",
            "circle",
            "date",
            "double precision",
            "inet",
            "integer",
            "interval",
            "line",
            "lseg",
            "macaddr",
            "money",
            "numeric",
            "path",
            "point",
            "polygon",
            "real",
            "smallint",
            "serial",
            "text",
            "time",
            "timestamp"

    };

    /**
     * 操作符转关键字
     * https://www.postgresql.org/docs/9.6/functions-subquery.html#FUNCTIONS-SUBQUERY-ANY-SOME
     */
    final static Map<String, String> operators2KeywordMap = new HashMap<String, String>() {
        {
            put("= ANY", "IN");
            put("<> ALL", "NOT IN");
            put("~~", "LIKE");
            put("!~~", "NOT LIKE");
            put("~~*", "ILIKE");
            put("!~~*", "NOT ILIKE");
        }
    };

    private final DatabaseInfo databaseInfo;

    public PgDatabaseDao(NgoxDbMaster ngoxDbMaster) {
        super(ngoxDbMaster);
        this.databaseInfo = this.initDatabaseInfo();
    }

    @Override
    protected DatabaseInfo initDatabaseInfo() {
        // https://www.postgresql.org/docs/13/datatype-character.html
        // String databaseName = ((DruidDataSource) this.getJdbcTemplate().getDataSource()).getDefaultCatalog();
        // DatabaseInfo databaseInfo = this.getJdbcTemplate().queryForList(
        //         Statement.QUERY_DATABASE_CHARSET.statement, DatabaseInfo.class, databaseName).get(0);

        // The storage requirement for a short string (up to 126 bytes) is 1 byte plus the actual string, which includes the space padding in the case of character.
        // Longer strings have 4 bytes of overhead instead of 1.
        // databaseInfo.setMaxLen(4);

        return null;
    }

    @Override
    public Logger getLogger() {
        return log;
    }

    @Override
    public String getSchema() {
        return "public";
    }

    @Override
    public DatabaseInfo getDatabaseInfo() {
        return this.databaseInfo;
    }

    @Override
    public List<ColumnInfo> getColumns(String... tableName) {
        List<ColumnInfo> columns = super.getColumns(tableName);
        columns.forEach(columnInfo -> {
            if (columnInfo.getColumnDef() != null) {
                // NULL::character varying
                for (String dataType : dataTypes) {
                    columnInfo.setColumnDef(columnInfo.getColumnDef().replaceAll(String.format("::%s[\\[\\]]?", dataType), ""));
                }

                if ("NULL".equals(columnInfo.getColumnDef())) {
                    columnInfo.setColumnDef(null);
                }
            }
        });
        return columns;
    }

    @Override
    public List<ConstraintInfo> getConstraintInfo(String... tableName) {
        // 获取检查约束
        // https://www.postgresql.org/docs/current/catalog-pg-constraint.html
        // https://www.postgresql.org/docs/9.5/functions-info.html
        // pg_get_constraintdef
        // https://www.postgresql.org/docs/9.6/ddl-constraints.html

        StringBuilder sqlBuilder = new StringBuilder(
                (tableName.length > 0 ? Statement.QUERY_CHECK_CONSTRAINT : Statement.QUERY_ALL_CHECK_CONSTRAINT).getStatement());

        Object[] params = this.getParams(sqlBuilder, tableName, null, null);

        final List<ConstraintInfo> resultList = this.getJdbcTemplate().queryForList(sqlBuilder.toString(), ConstraintInfo.class, params);

        resultList.forEach(ci -> {
            ci.setSourceCheckCondition(ci.getCheckCondition());
            ci.setCheckCondition(strip(ci.getCheckCondition()));
        });

        return resultList;
    }

    /**
     * 脱去检查约束中的 CHECK ()
     *
     * @param checkDefinition 检查约束定义
     * @return 约束定义
     */
    private static String strip(String checkDefinition) {

        // check
        checkDefinition = checkDefinition.replaceFirst("CHECK[\\s\n\r]*", "");

        for (Map.Entry<String, String> en : operators2KeywordMap.entrySet()) {
            checkDefinition = checkDefinition.replaceAll(StringUtils.escape(en.getKey()), en.getValue());
        }

        // array[]
        checkDefinition = checkDefinition.replaceAll("\\(ARRAY\\[", "(").replaceAll("]", "");

        // ::dataType
        for (String dataType : dataTypes) {
            checkDefinition = checkDefinition.replaceAll(String.format("::%s[\\[\\]]?", dataType), "");
        }

        return checkDefinition;
    }

    // /**
    //  * pgsql 默认用小写存储。
    //  * 查询语句：like '<TABLE_NAME>'
    //  * {@link org.postgresql.jdbc.PgDatabaseMetaData getTables}
    //  *
    //  * @param tableName 表名
    //  * @return 表的数量
    //  */
    // @Override
    // public int getTableCount(String tableName) {
    //     return super.getTableCount(tableName.toLowerCase());
    // }

}
