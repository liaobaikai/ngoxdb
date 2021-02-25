package com.liaobaikai.ngoxdb.types;

import com.liaobaikai.ngoxdb.enums.DatabaseVendorEnum;
import lombok.Getter;

import java.math.BigDecimal;
import java.sql.SQLType;
import java.sql.Types;

/**
 * @author baikai.liao
 * @Time 2021-02-03 19:22:11
 */
@Getter
public enum MsAccessType implements SQLType {

    UNKNOWN(Category.UNKNOWN, "unknown", 999, Object.class.getName(), 0L),

    TINYINT(Category.NUMERIC, "tinyint", java.sql.Types.TINYINT, Short.class.getName(), 3L),
    BIT(Category.NUMERIC, "bit", java.sql.Types.BIT, Boolean.class.getName(), 1L),
    SMALLINT(Category.NUMERIC, "smallint", java.sql.Types.SMALLINT, Short.class.getName(), 5L),
    INTEGER(Category.NUMERIC, "int", java.sql.Types.INTEGER, Integer.class.getName(), 10L),
    BIGINT(Category.NUMERIC, "bigint", java.sql.Types.BIGINT, Long.class.getName(), 19L),
    FLOAT(Category.NUMERIC, "float", Types.REAL, Double.class.getName(), 12L),
    DOUBLE(Category.NUMERIC, "float", Types.DOUBLE, Double.class.getName(), 12L),
    REAL(Category.NUMERIC, "real", java.sql.Types.REAL, Float.class.getName(), 12L),
    DECIMAL(Category.NUMERIC, "decimal", java.sql.Types.DECIMAL, BigDecimal.class.getName(), 28L),
    NUMERIC(Category.NUMERIC, "numeric", java.sql.Types.NUMERIC, BigDecimal.class.getName(), 28L),

    BOOLEAN(Category.NUMERIC, "boolean", Types.BOOLEAN, Boolean.class.getName(), 1L),

    DATETIME(Category.DATETIME, "datetime", Types.TIMESTAMP, java.sql.Timestamp.class.getName(), 26L),
    TIMESTAMP(Category.TIMESTAMP, "timestamp", Types.TIMESTAMP, java.sql.Timestamp.class.getName(), 26L),
    DATE(Category.DATE, "date", java.sql.Types.DATE, java.sql.Date.class.getName(), 10L),
    TIME(Category.TIME, "time", java.sql.Types.TIME, java.sql.Time.class.getName(), 16L),

    TEXT(Category.LONG_CHARACTER, "text", java.sql.Types.LONGVARCHAR, String.class.getName(), 65535L),

    CHAR(Category.CHARACTER, "char", Types.CHAR, String.class.getName(), 255L),
    VARCHAR(Category.CHARACTER, "varchar", Types.VARCHAR, String.class.getName(), 255L),
    CLOB(Category.LONG_CHARACTER, "clob", Types.CLOB, String.class.getName(), 0xffffffffL),
    LONGVARCHAR(Category.LONG_CHARACTER, "clob", Types.CLOB, String.class.getName(), 0xffffffffL),

    BINARY(Category.BINARY, "binary", Types.BINARY, "[B", 255L),
    VARBINARY(Category.BINARY, "varbinary", Types.VARBINARY, "[B", 255L),
    BLOB(Category.LONG_BINARY, "blob", Types.BLOB, String.class.getName(), 0xffffffffL),
    LONGVARBINARY(Category.LONG_BINARY, "blob", Types.BLOB, "[B", 0xffffffffL),

    /////////////////////////////////////////////////////////////////////////////////
    //             不支持的类型
    /////////////////////////////////////////////////////////////////////////////////
    NTEXT(Category.LONG_CHARACTER, "text", java.sql.Types.LONGVARCHAR, String.class.getName(), 255L),
    NCHAR(Category.CHARACTER, "char", Types.NCHAR, Character.class.getName(), 255L),
    NVARCHAR(Category.CHARACTER, "varchar", Types.NVARCHAR, String.class.getName(), 255L),
    NCLOB(Category.LONG_CHARACTER, "clob", Types.NCLOB, String.class.getName(), 0xffffffffL),
    LONGNVARCHAR(Category.LONG_CHARACTER, "clob", Types.NCLOB, String.class.getName(), 0xffffffffL),

    ;

    private final Category category;
    private final String accessType;
    private final int jdbcType;
    private final String javaClassName;
    private final Long precision;

    MsAccessType(Category catalog, String accessType, int jdbcType, String javaClassName, Long precision) {
        this.category = catalog;
        this.accessType = accessType;
        this.jdbcType = jdbcType;
        this.javaClassName = javaClassName;
        this.precision = precision;
    }

    @Override
    public String getName() {
        return this.accessType;
    }

    @Override
    public String getVendor() {
        return DatabaseVendorEnum.MICROSOFT_ACCESS.getVendor();
    }

    @Override
    public Integer getVendorTypeNumber() {
        return this.jdbcType;
    }

    public static MsAccessType getByJdbcType(int jdbcType){
        for(MsAccessType msAccessType: MsAccessType.values()){
            if(msAccessType.jdbcType == jdbcType){
                return msAccessType;
            }
        }
        return MsAccessType.UNKNOWN;
    }


    public enum Category {
        BINARY,
        CHARACTER,
        DATE,
        DATETIME,
        LONG_BINARY,
        LONG_CHARACTER,
        NUMERIC,
        UNKNOWN,
        TIME,
        TIMESTAMP
    }
}
