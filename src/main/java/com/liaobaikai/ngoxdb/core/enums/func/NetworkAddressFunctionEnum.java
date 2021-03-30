package com.liaobaikai.ngoxdb.core.enums.func;

/**
 * 网络地址函数
 * 参照：https://www.postgresql.org/docs/current/functions-net.html
 *
 * @author baikai.liao
 * @Time 2021-01-30 17:26:01
 */
public enum NetworkAddressFunctionEnum implements DatabaseFunctionEnum {

    // IP Address Functions
    abbrev,
    broadcast,
    family,
    host,
    hostmask,
    inet_merge,
    inet_same_family,
    masklen,
    netmask,
    network,
    set_masklen,
    text,

    // MAC Address Functions
    trunc,
    macaddr8_set7bit
}
