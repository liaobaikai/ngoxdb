package com.liaobaikai.ngoxdb.enums.fn;

/**
 * 日期类型函数
 * 参照：https://www.postgresql.org/docs/current/functions-formatting.html
 *
 * @author baikai.liao
 * @Time 2021-01-30 17:17:39
 */
public enum DateTypeFunctionEnum implements DatabaseFunctionEnum {

    // 函数
    // https://www.postgresql.org/docs/current/functions-formatting.html
    to_char,
    to_date,
    to_number,
    to_timestamp,

    // Date/Time Functions
    age,
    clock_timestamp,
    current_date,
    current_time,
    current_timestamp,
    date_part,
    date_trunc,
    extract,
    isfinite,
    justify_days,
    justify_hours,
    justify_interval,
    localtime,
    localtimestamp,
    make_date,
    make_interval,
    make_time,
    make_timestamp,
    make_timestamptz,
    now,
    statement_timestamp,
    timeofday,
    transaction_timestamp,
}
