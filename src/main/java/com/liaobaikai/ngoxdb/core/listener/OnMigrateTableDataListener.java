package com.liaobaikai.ngoxdb.core.listener;

import com.liaobaikai.ngoxdb.bean.info.TableInfo;

import java.util.List;

/**
 * @author baikai.liao
 * @Time 2021-03-02 15:10:25
 */
public interface OnMigrateTableDataListener {

    void onBeforeMigrateTableData(TableInfo ti, long tableRows, int pageSize);

    void onMigrateTableData(List<Object[]> batchArgs, TableInfo ti, int pageNum);

    void onAfterMigrateTableData(TableInfo ti, long tableRows);
}
