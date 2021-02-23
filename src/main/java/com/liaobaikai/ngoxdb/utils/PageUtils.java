package com.liaobaikai.ngoxdb.utils;

/**
 * 分页工具
 *
 * @author baikai.liao
 * @Time 2021-01-24 10:45:17
 */
public class PageUtils {

    /**
     * 获取页数
     * @param pageSize
     * @param total
     * @return
     */
    public static int getPageCount(int pageSize, long total){
        return (int) (total / pageSize + ((total % pageSize == 0) ? 0 : 1));
    }


}
