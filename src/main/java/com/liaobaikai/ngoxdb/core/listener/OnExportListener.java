package com.liaobaikai.ngoxdb.core.listener;

import com.liaobaikai.ngoxdb.bean.info.TableInfo;

import java.util.List;

/**
 * 导出监听
 *
 * @author baikai.liao
 * @Time 2021-03-02 15:10:25
 */
public interface OnExportListener {

    // /**
    //  * 导出所有表之前的回调函数
    //  */
    // void onBeforeDumpAll();
    //
    // /**
    //  * 导出表的行前的回调函数
    //  *
    //  * @param ti        表信息
    //  * @param tableRows 表的总行数
    //  * @param limit     导出的行数
    //  */
    // void onBeforeDumpRows(TableInfo ti, long tableRows, int limit);

    /**
     * 导出表的行的回调函数
     *
     * @param batchArgs 每行的数据
     * @param ti        表信息
     * @param offset    偏移量
     * @param limit     导入的行数
     */
    void exporting(List<Object[]> batchArgs, TableInfo ti, int offset, int limit);

    // /**
    //  * 导出所有数据后的回调函数
    //  */
    // void onAfterDumpAll();
}
