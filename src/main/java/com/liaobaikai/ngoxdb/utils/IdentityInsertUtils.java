package com.liaobaikai.ngoxdb.utils;

import com.liaobaikai.ngoxdb.boot.JdbcTemplate;
import com.microsoft.sqlserver.jdbc.ISQLServerConnection;
import dm.jdbc.driver.DmdbConnection;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author baikai.liao
 * @Time 2021-03-26 00:27:55
 */
public class IdentityInsertUtils {

    private static final Map<String, String> mapOfConnectionTableName = new HashMap<>();

    /**
     * 获取连接中的信息，IDENTITY_INSERT_TABLE_ON，查询是否设置了IDENTITY_INSERT，有的话，返回该表名，没有的话，设置并返回该表名
     *
     * @param expect       期望的表名
     * @param jdbcTemplate {@link JdbcTemplate}
     * @return 表名
     */
    public static String getAfterSetIfUndefined(String expect, JdbcTemplate jdbcTemplate) {

        return jdbcTemplate.execute(new ConnectionCallback<String>() {
            @Nullable
            @Override
            public String doInConnection(@NonNull Connection con) throws SQLException, DataAccessException {

                if (con.isWrapperFor(DmdbConnection.class)) {
                    DmdbConnection dmdbConnection = con.unwrap(DmdbConnection.class);

                } else if (con.isWrapperFor(ISQLServerConnection.class)) {
                    ISQLServerConnection isqlServerConnection = con.unwrap(ISQLServerConnection.class);
                    String value = mapOfConnectionTableName.get(isqlServerConnection.toString());
                    if (value != null) {
                        return value;
                    }
                    mapOfConnectionTableName.put(isqlServerConnection.toString(), expect);
                    System.out.println(mapOfConnectionTableName);
                }
                return expect;
            }
        });
    }

    public static void remove(JdbcTemplate jdbcTemplate) {
        jdbcTemplate.execute(new ConnectionCallback<String>() {
            @Nullable
            @Override
            public String doInConnection(@NonNull Connection con) throws SQLException, DataAccessException {
                if (con.isWrapperFor(DmdbConnection.class)) {
                    DmdbConnection dmdbConnection = con.unwrap(DmdbConnection.class);
                } else if (con.isWrapperFor(ISQLServerConnection.class)) {
                    ISQLServerConnection isqlServerConnection = con.unwrap(ISQLServerConnection.class);
                    mapOfConnectionTableName.remove(isqlServerConnection.toString());
                    System.out.println(mapOfConnectionTableName);
                }
                return null;
            }
        });
    }
}
