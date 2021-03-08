package com.liaobaikai.ngoxdb.core.listener;

import com.liaobaikai.ngoxdb.bean.info.TableInfo;

import java.util.List;

/**
 * 检查表的一致性监听
 *
 * @author baikai.liao
 * @Time 2021-03-03 13:46:41
 */
public interface OnComparingListener {

    /**
     * 获取源端的某页(所有)数据
     *
     * @param ti            表信息
     * @param tableRowCount 表行总数
     * @param offset        进行分页查询
     * @param limit         进行分页查询
     * @param srcRowArgs    源端数据
     */
    void onComparing(TableInfo ti, long tableRowCount, int offset, int limit, List<Object[]> srcRowArgs);
}
