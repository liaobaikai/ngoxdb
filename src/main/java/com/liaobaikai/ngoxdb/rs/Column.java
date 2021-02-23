package com.liaobaikai.ngoxdb.rs;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Getter;
import lombok.Setter;

import java.sql.DatabaseMetaData;

/**
 * @author baikai.liao
 * @Time 2021-01-27 15:23:44
 */
@Setter
@Getter
public class Column extends TableBase{

    /**
     * 列名
     */
    private String columnName;

    /**
     * {@link java.sql.Types}
     * {@link microsoft.sql.Types}
     */
    private int dataType;

    /**
     * 类型名称
     */
    private String typeName;

    /**
     * 列的大小
     */
    private int columnSize;

    // is not used.
    private int bufferLength;

    /**
     * 小数位数。 对于DECIMAL_DIGITS不适用的数据类型，返回空值。
     */
    private int decimalDigits;

    /**
     * 基数（通常为10或2）
     */
    private int numPrecRadix;

    /**
     * 字段可空
     * 0: 不能为空
     * 1: 可以为空
     * 2: 未知
     */
    private int nullable = DatabaseMetaData.columnNoNulls;

    /**
     * 列注释
     */
    private String remarks;

    /**
     * 列默认值
     */
    private String columnDef;

    /**
     * unused
     */
    private int sqlDataType;

    /**
     * unused
     */
    private int sqlDatetimeSub;

    /**
     * 对于char类型，列中的最大字节数
     */
    private int charOctetLength;

    /**
     * 列序号，从1开始
     */
    private int ordinalPosition;

    /**
     * 是否为null
     * YES: 是
     * NO:  否
     * '' | null:  未知
     */
    private String isNullable;

    /**
     * catalog of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF)
     */
    private String scopeCatalog;

    /**
     * schema of table that is the scope of a reference attribute (null if the DATA_TYPE isn't REF)
     */
    private String scopeSchema;

    /**
     * table name that this the scope of a reference attribute (null if the DATA_TYPE isn't REF)
     */
    private String scopeTable;

    /**
     * source type of a distinct type or user-generated Ref type, SQL type from java.sql.Types (null if DATA_TYPE isn't DISTINCT or user-generated REF)
     */
    private String sourceDataType;

    /**
     * 是否为自动增长列
     * YES：是
     * NO：否
     * '' | null:  未知
     */
    private String isAutoincrement;

    /**
     * 是否为自动生成列
     * YES：是
     * NO：否
     * '' | null:  未知
     */
    private String isGeneratedColumn;

    public static final String YES = "YES";
    public static final String NO = "NO";
    public static final String UNKNOWN = "UNKNOWN";

}
