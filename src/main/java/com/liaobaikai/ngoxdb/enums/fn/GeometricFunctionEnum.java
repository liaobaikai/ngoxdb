package com.liaobaikai.ngoxdb.enums.fn;

/**
 * 空间函数
 * 参照：https://www.postgresql.org/docs/current/functions-geometry.html
 * @author baikai.liao
 * @Time 2021-01-30 17:23:22
 */
public enum GeometricFunctionEnum implements DatabaseFunctionEnum {

    // Geometric Functions
    area,
    center,
    diagonal,
    diameter,
    height,
    isclosed,
    isopen,
    length,
    npoints,
    pclose,
    popen,
    radius,
    slope,
    width,

    // Geometric Type Conversion Functions
    box,
    bound_box,
    circle,
    line,
    lseg,
    path,
    point,
    polygon,

}
