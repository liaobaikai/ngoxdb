package com.liaobaikai.ngoxdb.core.enums.fn;

/**
 * 字符串函数
 * 参照：https://www.postgresql.org/docs/current/functions-string.html
 *
 * @author baikai.liao
 * @Time 2021-01-30 17:10:04
 */
public enum StringFunctionEnum implements DatabaseFunctionEnum {

    // SQL String Functions
    bit_length,
    char_length,
    character_length,
    lower,
    normalize,
    octet_length,
    overlay,
    position,
    substring,
    trim,
    upper,

    // Other String Functions
    ascii,
    btrim,
    chr,
    concat,
    concat_ws,
    format,
    initcap,
    left,
    length,
    lpad,
    ltrim,
    md5,
    parse_ident,
    quote_ident,
    quote_literal,
    quote_nullable,
    regexp_match,
    regexp_matches,
    regexp_replace,
    regexp_split_to_array,
    regexp_split_to_table,
    repeat,
    replace,
    reverse,
    right,
    rpad,
    rtrim,
    split_part,
    strpos,
    substr,
    starts_with,
    to_ascii,
    to_hex,
    translate,

    // Text/Binary String Conversion Functions
    convert,
    convert_from,
    convert_to,
    encode,
    decode,

    // uuid
    uuid,
    // ERROR:  function gen_random_uuid() does not exist
    // LINE 1: select gen_random_uuid();
    //                ^
    // HINT:  No function matches the given name and argument types. You might need to add explicit type casts.
    // CREATE EXTENSION pgcrypto;
//     postgres=# select gen_random_uuid();
//     gen_random_uuid
// --------------------------------------
//     b9babd56-decd-4503-85ed-5250da5c2530
}
