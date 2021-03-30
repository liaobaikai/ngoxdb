package com.liaobaikai.ngoxdb.bean;

import lombok.Getter;
import lombok.Setter;

/**
 * 分页信息
 *
 * @author baikai.liao
 * @Time 2021-03-10 20:02:34
 */
@Setter
@Getter
public class PreparedPagination {

    /**
     * SQL语句
     */
    private String preparedSql;

    /**
     * 参数值
     */
    private Object[] paramValues;

    public PreparedPagination() {
    }

    public PreparedPagination(String preparedSql, Object[] paramValues) {
        this.preparedSql = preparedSql;
        this.paramValues = paramValues;
    }
}
