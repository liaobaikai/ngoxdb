package com.liaobaikai.ngoxdb.types;

import com.liaobaikai.ngoxdb.enums.DatabaseVendorEnum;
import lombok.Getter;

import java.math.BigDecimal;
import java.sql.SQLType;
import java.sql.Types;

/**
 * copy from: com.microsoft.sqlserver.jdbc.DataTypes
 */
@Getter
public enum SQLServerType implements SQLType {

    UNKNOWN(Category.UNKNOWN, "unknown", 999, Object.class.getName()),
    TINYINT(Category.NUMERIC, "tinyint", java.sql.Types.TINYINT, Short.class.getName()),
    BIT(Category.NUMERIC, "bit", java.sql.Types.BIT, Boolean.class.getName()),
    SMALLINT(Category.NUMERIC, "smallint", java.sql.Types.SMALLINT, Short.class.getName()),
    INTEGER(Category.NUMERIC, "int", java.sql.Types.INTEGER, Integer.class.getName()),
    BIGINT(Category.NUMERIC, "bigint", java.sql.Types.BIGINT, Long.class.getName()),
    FLOAT(Category.NUMERIC, "float", java.sql.Types.DOUBLE, Double.class.getName()),
    REAL(Category.NUMERIC, "real", java.sql.Types.REAL, Float.class.getName()),
    SMALLDATETIME(Category.DATETIME, "smalldatetime", microsoft.sql.Types.SMALLDATETIME, java.sql.Timestamp.class.getName()),
    DATETIME(Category.DATETIME, "datetime", microsoft.sql.Types.DATETIME, java.sql.Timestamp.class.getName()),
    DATE(Category.DATE, "date", java.sql.Types.DATE, java.sql.Date.class.getName()),
    TIME(Category.TIME, "time", java.sql.Types.TIME, java.sql.Time.class.getName()),
    DATETIME2(Category.DATETIME2, "datetime2", java.sql.Types.TIMESTAMP, java.sql.Timestamp.class.getName()),
    DATETIMEOFFSET(Category.DATETIMEOFFSET, "datetimeoffset", microsoft.sql.Types.DATETIMEOFFSET, microsoft.sql.DateTimeOffset.class.getName()),
    SMALLMONEY(Category.NUMERIC, "smallmoney", microsoft.sql.Types.SMALLMONEY, BigDecimal.class.getName()),
    MONEY(Category.NUMERIC, "money", microsoft.sql.Types.MONEY, BigDecimal.class.getName()),
    CHAR(Category.CHARACTER, "char", java.sql.Types.CHAR, String.class.getName()),
    VARCHAR(Category.CHARACTER, "varchar", java.sql.Types.VARCHAR, String.class.getName()),
    VARCHARMAX(Category.LONG_CHARACTER, "varchar", java.sql.Types.LONGVARCHAR, String.class.getName()),
    TEXT(Category.LONG_CHARACTER, "text", java.sql.Types.LONGVARCHAR, String.class.getName()),
    NCHAR(Category.NCHARACTER, "nchar", -15, String.class.getName()),
    NVARCHAR(Category.NCHARACTER, "nvarchar", -9, String.class.getName()),
    NVARCHARMAX(Category.LONG_NCHARACTER, "nvarchar", -16, String.class.getName()),
    NTEXT(Category.LONG_NCHARACTER, "ntext", -16, String.class.getName()),
    BINARY(Category.BINARY, "binary", java.sql.Types.BINARY, "[B"),
    VARBINARY(Category.BINARY, "varbinary", java.sql.Types.VARBINARY, "[B"),
    VARBINARYMAX(Category.LONG_BINARY, "varbinary", java.sql.Types.LONGVARBINARY, "[B"),
    IMAGE(Category.LONG_BINARY, "image", java.sql.Types.LONGVARBINARY, "[B"),
    DECIMAL(Category.NUMERIC, "decimal", java.sql.Types.DECIMAL, BigDecimal.class.getName()),
    NUMERIC(Category.NUMERIC, "numeric", java.sql.Types.NUMERIC, BigDecimal.class.getName()),
    GUID(Category.GUID, "uniqueidentifier", microsoft.sql.Types.GUID, String.class.getName()),
    SQL_VARIANT(Category.SQL_VARIANT, "sql_variant", microsoft.sql.Types.SQL_VARIANT, Object.class.getName()),
    UDT(Category.UDT, "udt", java.sql.Types.VARBINARY, "[B"),
    XML(Category.XML, "xml", -16, String.class.getName()),
    TIMESTAMP(Category.TIMESTAMP, "timestamp", Types.TIMESTAMP, java.sql.Timestamp.class.getName()),
    GEOMETRY(Category.UDT, "geometry", microsoft.sql.Types.GEOMETRY, Object.class.getName()),
    GEOGRAPHY(Category.UDT, "geography", microsoft.sql.Types.GEOGRAPHY, Object.class.getName()),

    ;

    final Category category;
    private final String name;
    private final int jdbcType;
    private final String javaClassName;

    SQLServerType(Category category, String name, int jdbcType, String javaClassName) {
        this.category = category;
        this.name = name;
        this.jdbcType = jdbcType;
        this.javaClassName = javaClassName;
    }

    public String toString() {
        return name;
    }

    public static SQLServerType getByName(String sqlServerTypeName) {

        for (SQLServerType sqlServerType : SQLServerType.values()) {
            if (sqlServerTypeName.equals(sqlServerType.name)) {
                return sqlServerType;
            }
        }
        return SQLServerType.UNKNOWN;
    }

    public static SQLServerType getByJdbcType(int jdbcType) {

        for (SQLServerType sqlServerType : SQLServerType.values()) {
            if (sqlServerType.getJdbcType() == jdbcType) {
                return sqlServerType;
            }
        }
        return SQLServerType.UNKNOWN;
    }

    @Override
    public String getVendor() {
        return DatabaseVendorEnum.SQLSERVER.getVendor();
    }

    @Override
    public Integer getVendorTypeNumber() {
        return this.jdbcType;
    }

    enum Category {
        BINARY,
        CHARACTER,
        DATE,
        DATETIME,
        DATETIME2,
        DATETIMEOFFSET,
        GUID,
        LONG_BINARY,
        LONG_CHARACTER,
        LONG_NCHARACTER,
        NCHARACTER,
        NUMERIC,
        UNKNOWN,
        TIME,
        TIMESTAMP,
        UDT,
        SQL_VARIANT,
        XML;

    }


    public static final BigDecimal MAX_VALUE_MONEY = new BigDecimal("922337203685477.5807");
    public static final BigDecimal MIN_VALUE_MONEY = new BigDecimal("-922337203685477.5808");
    public static final BigDecimal MAX_VALUE_SMALLMONEY = new BigDecimal("214748.3647");
    public static final BigDecimal MIN_VALUE_SMALLMONEY = new BigDecimal("-214748.3648");

    /**
     * Max length in Unicode characters allowed by the "short" NVARCHAR type. Values longer than this must use
     * NVARCHAR(max) (Yukon or later) or NTEXT (Shiloh)
     */
    public final static int SHORT_VARTYPE_MAX_CHARS = 4000;

    /**
     * Max length in bytes allowed by the "short" VARBINARY/VARCHAR types. Values longer than this must use
     * VARBINARY(max)/VARCHAR(max) (Yukon or later) or IMAGE/TEXT (Shiloh)
     */
    public final static int SHORT_VARTYPE_MAX_BYTES = 8000;

    /**
     * A type with unlimited max size, known as varchar(max), varbinary(max) and nvarchar(max), which has a max size of
     * 0xFFFF, defined by PARTLENTYPE.
     */
    public final static int SQL_USHORTVARMAXLEN = 65535; // 0xFFFF

    /**
     * From SQL Server 2005 Books Online : ntext, text, and image (Transact-SQL)
     * http://msdn.microsoft.com/en-us/library/ms187993.aspx
     * <p>
     * image "... through 2^31 - 1 (2,147,483,687) bytes."
     * <p>
     * text "... maximum length of 2^31 - 1 (2,147,483,687) characters."
     * <p>
     * ntext "... maximum length of 2^30 - 1 (1,073,741,823) characters."
     */
    public final static int NTEXT_MAX_CHARS = 0x3FFFFFFF;
    public final static int IMAGE_TEXT_MAX_BYTES = 0x7FFFFFFF;

    /**
     * Transact-SQL Data Types: http://msdn.microsoft.com/en-us/library/ms179910.aspx
     * <p>
     * varbinary(max) "max indicates that the maximum storage size is 2^31 - 1 bytes. The storage size is the actual
     * length of the data entered + 2 bytes."
     * <p>
     * varchar(max) "max indicates that the maximum storage size is 2^31 - 1 bytes. The storage size is the actual
     * length of the data entered + 2 bytes."
     * <p>
     * nvarchar(max) "max indicates that the maximum storage size is 2^31 - 1 bytes. The storage size, in bytes, is two
     * times the number of characters entered + 2 bytes."
     * <p>
     * Normally, that would mean that the maximum length of nvarchar(max) data is 0x3FFFFFFE characters and that the
     * maximum length of varchar(max) or varbinary(max) data is 0x3FFFFFFD bytes. However... Despite the documentation,
     * SQL Server returns 2^30 - 1 and 2^31 - 1 respectively as the PRECISION of these types, so use that instead.
     */
    public final static int MAX_VARTYPE_MAX_CHARS = 0x3FFFFFFF;
    public final static int MAX_VARTYPE_MAX_BYTES = 0x7FFFFFFF;

    // Special length indicator for varchar(max), nvarchar(max) and varbinary(max).
    public static final int MAXTYPE_LENGTH = 0xFFFF;

    public static final int UNKNOWN_STREAM_LENGTH = -1;


    // public static String getColumnType(String typeName, int ){
    //
    // }
}