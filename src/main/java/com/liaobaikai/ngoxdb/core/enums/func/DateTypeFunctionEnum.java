package com.liaobaikai.ngoxdb.core.enums.func;

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
    to_date,    // to_date('2005-12-25,13:25:59','yyyy-mm-dd,hh24:mi:ss')
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

    last_day,   // 最后一天，返回日期

    datetime_offset,    // 带时区
    utc_datetime,       // utc 日期时间
    utc_date,           // utc 日期
    utc_time,           // utc 时间

    ;
}
