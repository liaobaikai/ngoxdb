package com.liaobaikai.ngoxdb.core.service;

import com.liaobaikai.ngoxdb.bean.ComparisonResult;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.core.listener.OnComparingListener;
import org.slf4j.Logger;

import java.util.List;

/**
 * 数据库比较器
 *
 * @author baikai.liao
 * @Time 2021-03-04 14:31:04
 */
public interface DatabaseComparator {

    /**
     * 获取logger
     * @return Logger
     */
    Logger getLogger();

    /**
     * 获取转换器
     *
     * @return DatabaseConverter
     */
    DatabaseConverter getDatabaseConverter();

    /**
     * 获取比较结果
     *
     * @return 比较结果列表
     */
    List<ComparisonResult> getComparisonResultList();

    /**
     * 比较所有表
     *
     * @param tis                 表信息列表
     * @param onComparingListener 比较监听
     */
    void compareAllTables(List<TableInfo> tis, OnComparingListener onComparingListener);


    /**
     * 比较数据的一致性
     *
     * @param ti         表信息
     * @param sourceArgs 源数据
     * @param targetArgs 目标数据
     * @return 比较结果
     */
    ComparisonResult compare(TableInfo ti, List<Object[]> sourceArgs, List<Object[]> targetArgs);

    /**
     * 比较数据的一致性
     *
     * @param ti             表信息
     * @param sourceArgs     源数据
     * @param targetArgs     目标数据
     * @param uniqueKeyIndex 唯一键索引
     * @return 比较结果
     */
    ComparisonResult compare(TableInfo ti, List<Object[]> sourceArgs, List<Object[]> targetArgs, int uniqueKeyIndex);

}
