package com.liaobaikai.ngoxdb.bean;

import com.liaobaikai.ngoxdb.core.constant.JdbcDataType;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * 列转换信息
 *
 * @author baikai.liao
 * @Time 2021-03-11 23:41:45
 */
@Setter
@Getter
public class ColumnType {

    /**
     * 列名
     */
    private String columnName;

    /**
     * 转换后的jdbc type
     * {@link com.liaobaikai.ngoxdb.core.constant.JdbcDataType}
     */
    private int jdbcDataType = Integer.MIN_VALUE;

    /**
     * 类型名称
     */
    private String typeName = "UNKNOWN";

    /**
     * 字符、字节长度
     */
    private int length = -1;

    /**
     * 精度
     */
    private int precision = -1;

    /**
     * 刻度
     */
    private int scale = -1;

    /**
     * 是否为无符号
     */
    private boolean isUnsigned;

    /**
     * 类型名称映射 {@link JdbcDataType}
     */
    private final Map<Integer, String> dataTypeMap;

    /**
     * 是否从数据库查出来
     */
    private boolean fromDb;

    public ColumnType() {
        this.dataTypeMap = null;
    }

    public ColumnType(Map<Integer, String> dataTypeMap) {
        this.dataTypeMap = dataTypeMap;
    }

    public void setJdbcDataType(int jdbcDataType) {
        this.jdbcDataType = jdbcDataType;
        if (this.dataTypeMap != null) {
            this.typeName = this.dataTypeMap.get(jdbcDataType);
        }
    }

    @Override
    public String toString() {

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(this.typeName);

        if (this.length > 0) {
            stringBuilder.append("(").append(length).append(")");
        } else if (this.precision > 0) {
            stringBuilder.append("(").append(this.precision);
            if (this.scale > 0) {
                stringBuilder.append(", ").append(this.scale);
            }
            stringBuilder.append(")");
        }

        if (this.isUnsigned) {
            stringBuilder.append(" unsigned ");
        }
        return stringBuilder.toString();
    }
}
