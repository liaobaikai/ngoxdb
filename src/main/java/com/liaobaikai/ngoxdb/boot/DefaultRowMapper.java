package com.liaobaikai.ngoxdb.boot;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * @author baikai.liao
 * @Time 2021-03-17 23:09:39
 */
public class DefaultRowMapper<T> implements RowMapper<T> {

    private final Class<T> elementType;

    public DefaultRowMapper(Class<T> elementType) {
        this.elementType = elementType;
    }

    @Nullable
    @Override
    public T mapRow(@NonNull ResultSet rs, int rowNum) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        JSONObject mapOfColumnValues = new JSONObject();
        for (int i = 1; i <= columnCount; i++) {
            String column = JdbcUtils.lookupColumnName(rsmd, i);
            mapOfColumnValues.putIfAbsent(column, JdbcUtils.getResultSetValue(rs, i));
        }

        return JSON.toJavaObject(mapOfColumnValues, elementType);
    }

}
