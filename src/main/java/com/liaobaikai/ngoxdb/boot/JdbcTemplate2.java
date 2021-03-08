package com.liaobaikai.ngoxdb.boot;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.liaobaikai.ngoxdb.core.config.DatabaseConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.lang.NonNull;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Blob;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * @author baikai.liao
 * @Time 2021-01-23 12:01:10
 */
public class JdbcTemplate2 extends JdbcTemplate {

    // 控制台打印jdbc数据库操作信息
    private boolean isDbConsoleLog = true;

    private DatabaseConfig databaseConfig;

    public JdbcTemplate2() {

    }

    public JdbcTemplate2(DataSource dataSource) {
        super(dataSource);
    }

    public JdbcTemplate2(DataSource dataSource, boolean lazyInit) {
        super(dataSource, lazyInit);
    }

    public JdbcTemplate2(DatabaseConfig databaseConfig) {
        this.databaseConfig = databaseConfig;
    }

    public JdbcTemplate2(DataSource dataSource, DatabaseConfig databaseConfig) {
        this(dataSource, true, databaseConfig);
    }

    public JdbcTemplate2(DataSource dataSource, boolean lazyInit, DatabaseConfig databaseConfig) {
        super(dataSource, lazyInit);
        this.databaseConfig = databaseConfig;
        this.isDbConsoleLog = databaseConfig.isDbConsoleLog();
    }

    public DatabaseConfig getDatabaseConfig() {
        return databaseConfig;
    }


    /**
     * 查询返回对象列表
     *
     * @param sql  SQL
     * @param args 参数
     * @return
     */
    public <T> List<T> queryForList2(String sql, Class<T> clazz, Object... args) {
        List<LinkedHashMap<String, Object>> queryResponse = this.query(sql, args);
        List<T> resultList = new ArrayList<>();
        queryResponse.forEach(rowMap -> resultList.add(JSON.toJavaObject((JSON) JSONObject.toJSON(rowMap), clazz)));
        return resultList;
    }

    /**
     * 查询
     *
     * @param sql  SQL
     * @param args 参数
     * @return List<LinkedHashMap>
     */
    public List<LinkedHashMap<String, Object>> query(String sql, Object... args) {
        printConnectionInfo();
        printDaoInfo();
        printPreparingInfo(sql);
        printParameterInfo(args);

        StringBuilder columnsBuilder = new StringBuilder();
        List<LinkedHashMap<String, Object>> queryResponse = this.query(sql, (rs, rowNum) -> {
            LinkedHashMap<String, Object> row = new LinkedHashMap<>();
            StringBuilder valueBuilder = new StringBuilder();
            final int columnCount = rs.getMetaData().getColumnCount();
            String columnName;
            Object columnValue;
            for (int i = 0; i < columnCount; i++) {
                columnName = JdbcUtils.lookupColumnName(rs.getMetaData(), i + 1);
                columnValue = rs.getObject(i + 1);
                if (rowNum == 0) {
                    columnsBuilder.append(columnName).append(", ");
                }
                row.put(columnName, columnValue);
                if (columnValue instanceof InputStream
                        || columnValue instanceof Blob) {
                    valueBuilder.append("<<BLOB>>");
                } else {
                    valueBuilder.append(columnValue == null ? "NULL" : columnValue.toString()).append(", ");
                }

            }

            if (rowNum == 0) {
                columnsBuilder.delete(columnsBuilder.length() - 2, columnsBuilder.length());
                this.printColumnInfo(columnsBuilder.toString());
            }

            valueBuilder.delete(valueBuilder.length() - 2, valueBuilder.length());

            this.printRowInfo(valueBuilder.toString());

            return row;
        }, args);

        printTotalInfo(queryResponse.size());
        return queryResponse;
    }


    @Override
    public void execute(@NonNull String sql) throws DataAccessException {
        printConnectionInfo();
        printDaoInfo();
        printExecutingInfo(sql);
        super.execute(sql);
    }

    @Override
    public int update(@NonNull String sql, Object... args) throws DataAccessException {
        printConnectionInfo();
        printDaoInfo();
        printPreparingInfo(sql);
        printParameterInfo(args);
        return super.update(sql, args);
    }


    private void printConnectionInfo() {
        if (this.isDbConsoleLog) {
            if (this.getDataSource() == null) {
                return;
            }
            final HikariDataSource ds = (HikariDataSource) this.getDataSource();
            int index;
            if ((index = this.databaseConfig.getUrl().indexOf("?")) != -1) {
                System.out.println("==> Connection: " + ds + ", Database: " + this.databaseConfig.getUrl().substring(0, index));
            } else {
                System.out.println("==> Connection: " + ds + ", Database: " + this.databaseConfig.getUrl());
            }

        }
    }

    private void printDaoInfo() {
        if (this.isDbConsoleLog) {
            StackTraceElement stackTraceElement = Thread.currentThread().getStackTrace()[5];
            System.out.println("==>   DaoClass: " + stackTraceElement.getClassName() + "." + stackTraceElement.getMethodName() + "() [LineNumber: " + stackTraceElement.getLineNumber());
        }
    }


    private void printPreparingInfo(final String sql) {
        if (this.isDbConsoleLog) {
            System.out.println("==>  Preparing: " + sql.replaceAll("\n", " ").replaceAll(" +", " "));
        }
    }

    private void printExecutingInfo(final String sql) {
        if (this.isDbConsoleLog) {
            System.out.println("==>  Executing: " + sql.replaceAll("\n", " ").replaceAll(" +", " "));
        }

    }


    private void printParameterInfo(Object[] args) {

        if (this.isDbConsoleLog) {
            StringBuilder parameterBuilder = new StringBuilder();
            if (args != null) {
                for (Object o : args) {
                    try {
                        int index = o.getClass().getName().lastIndexOf(".") + 1;
                        String typeName = o.getClass().getName().substring(index);
                        parameterBuilder.append(o).append("(").append(typeName).append("), ");
                    } catch (Exception ignored) {
                    }

                }
                if (parameterBuilder.length() > 0) {
                    parameterBuilder.delete(parameterBuilder.length() - 2, parameterBuilder.length());
                }
            }

            System.out.println("==> Parameters: " + parameterBuilder.toString());
        }

    }

    private void printColumnInfo(String columnNames) {
        if (this.isDbConsoleLog) {
            System.out.println("<==    Columns: " + columnNames);
        }
    }

    private void printRowInfo(String columnValues) {
        if (this.isDbConsoleLog) {
            System.out.println("<==        Row: " + columnValues);
        }
    }

    private void printTotalInfo(int total) {
        if (this.isDbConsoleLog) {
            System.out.println("<==      Total: " + total);
        }

    }

}
