package com.liaobaikai.ngoxdb.core.func.impl;

import com.liaobaikai.ngoxdb.core.func.SQLFunction;
import lombok.Getter;

/**
 * 无参的行数
 *
 * @author baikai.liao
 * @Time 2021-03-12 12:19:03
 */
@Getter
public class NoArgumentSQLFunction implements SQLFunction {

    /**
     * function name
     */
    private final String name;

    /**
     * {@link com.liaobaikai.ngoxdb.core.constant.JdbcDataType}
     */
    private final int returnType;

    private final boolean hasParentheses;

    public NoArgumentSQLFunction(String name, int returnType) {
        this(name, returnType, true);
    }

    public NoArgumentSQLFunction(String name, boolean hasParentheses) {
        this(name, Integer.MIN_VALUE, hasParentheses);
    }

    public NoArgumentSQLFunction(String name, int returnType, boolean hasParentheses) {
        this.name = name;
        this.returnType = returnType;
        this.hasParentheses = hasParentheses;
    }

    @Override
    public boolean hasArguments() {
        return false;
    }

    @Override
    public boolean hasParentheses() {
        return hasParentheses;
    }

    @Override
    public String build() {
        return hasParentheses ? name + "()" : name;
    }
}
