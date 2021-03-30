package com.liaobaikai.ngoxdb.core.listener;

/**
 * 并行监听器
 *
 * @author baikai.liao
 * @Time 2021-03-18 17:49:33
 */
public interface ParallelCallback {

    void callback(int index);
}
