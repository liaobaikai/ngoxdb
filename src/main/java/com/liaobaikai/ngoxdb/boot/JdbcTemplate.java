package com.liaobaikai.ngoxdb.boot;

import org.springframework.dao.DataAccessException;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import javax.sql.DataSource;
import java.util.List;

/**
 * @author baikai.liao
 * @Time 2021-03-17 22:41:27
 */
public class JdbcTemplate extends org.springframework.jdbc.core.JdbcTemplate {

    public JdbcTemplate() {
    }

    public JdbcTemplate(DataSource dataSource) {
        super(dataSource);
    }

    public JdbcTemplate(DataSource dataSource, boolean lazyInit) {
        super(dataSource, lazyInit);
    }

    @NonNull
    @Override
    public <T> List<T> queryForList(@NonNull String sql, @NonNull Class<T> elementType) throws DataAccessException {
        return super.query(sql, new DefaultRowMapper<>(elementType));
    }

    @NonNull
    @Override
    public <T> List<T> queryForList(@NonNull String sql, @NonNull Class<T> elementType, @Nullable Object... args) throws DataAccessException {
        // List<Map<String, Object>> rows = this.queryForList(sql, args);
        // List<T> finalRows = new ArrayList<>();
        // for(Map<String, Object> row : rows){
        //     finalRows.add(JSON.toJavaObject((JSON) JSON.toJSON(row), elementType));
        // }
        // return finalRows;
        return super.query(sql, args, new DefaultRowMapper<>(elementType));
    }
}
