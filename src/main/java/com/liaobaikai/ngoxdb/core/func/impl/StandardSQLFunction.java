package com.liaobaikai.ngoxdb.core.func.impl;

import com.liaobaikai.ngoxdb.core.func.SQLFunction;
import lombok.Getter;

/**
 * 标准的函数
 *
 * @author baikai.liao
 * @Time 2021-03-12 12:18:10
 */
@Getter
public class StandardSQLFunction implements SQLFunction {

    /**
     * function name
     */
    private final String name;

    /**
     * {@link com.liaobaikai.ngoxdb.core.constant.JdbcDataType}
     */
    private final int returnType;

    public StandardSQLFunction(String name) {
        this(name, Integer.MIN_VALUE);
    }

    public StandardSQLFunction(String name, int returnType) {
        this.name = name;
        this.returnType = returnType;
    }

    @Override
    public boolean hasArguments() {
        return true;
    }

    @Override
    public boolean hasParentheses() {
        return true;
    }

    @Override
    public String build() {
        return name;
    }
}
