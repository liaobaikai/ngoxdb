package com.liaobaikai.ngoxdb.core.func;

/**
 * @author baikai.liao
 * @Time 2021-03-12 13:50:04
 */
public interface SQLFunction {

    /**
     * 是否含有参数
     *
     * @return
     */
    boolean hasArguments();

    /**
     * 是否含有圆括号
     *
     * @return
     */
    boolean hasParentheses();

    /**
     * 构建
     *
     * @return
     */
    String build();
}
