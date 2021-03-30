package com.liaobaikai.ngoxdb.core.func.impl;

import com.liaobaikai.ngoxdb.utils.StringUtils;

/**
 * 动态函数
 *
 * @author baikai.liao
 * @Time 2021-03-12 14:24:09
 */
public class DynamicSQLFunction extends StandardSQLFunction {

    private final String[] arguments;

    public DynamicSQLFunction(String name, String... arguments) {
        super(name);
        this.arguments = arguments;
    }

    public DynamicSQLFunction(String name, int returnType, String... arguments) {
        super(name, returnType);
        this.arguments = arguments;
    }

    @Override
    public String build() {
        StringBuilder sBuilder = new StringBuilder(getName());
        if (this.hasParentheses()) {
            sBuilder.append("(");
        }
        sBuilder.append(StringUtils.join(',', arguments));
        if (this.hasParentheses()) {
            sBuilder.append(")");
        }
        return sBuilder.toString();
    }
}
