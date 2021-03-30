package com.liaobaikai.ngoxdb.core.enums.func;

/**
 * 数学函数
 * 参照：https://www.postgresql.org/docs/current/functions-math.html
 *
 * @author baikai.liao
 * @Time 2021-01-30 16:58:48
 */
public enum MathFunctionEnum implements DatabaseFunctionEnum {

    // Mathematical Functions
    abs,
    cbrt,
    ceil,
    ceiling,
    degrees,
    div,
    exp,
    factorial,
    floor,
    gcd,
    lcm,
    ln,
    log,
    log10,
    min_scale,
    mod,
    pi,
    power,
    radians,
    round,
    scale,
    sign,
    sqrt,
    trunc,
    width_bucket,

    // Random Functions
    random,

    // Trigonometric Functions
    acos,
    acosd,
    asin,
    asind,
    atan,
    atand,
    atan2,
    atan2d,
    cos,
    cosd,
    cot,
    cotd,
    sin,
    sind,
    tan,
    tand,


    ////
    bitand, // 位且

    ;

}
