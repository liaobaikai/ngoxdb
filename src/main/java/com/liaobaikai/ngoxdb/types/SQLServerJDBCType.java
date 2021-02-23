package com.liaobaikai.ngoxdb.types;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * SQLServer的类型
 * https://docs.microsoft.com/zh-cn/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15
 *
 * @author baikai.liao
 * @Time 2021-01-18 23:41:06
 */
// public enum SQLServerType implements SQLType {

// BIGINT("BIGINT", Types.BIGINT, Long.class, SQLServerType.IS_DECIMAL, 19L),
// NUMERIC("NUMERIC", Types.NUMERIC, BigDecimal.class, SQLServerType.IS_DECIMAL, 38L),
// DECIMAL("DECIMAL", Types.NUMERIC, BigDecimal.class, SQLServerType.IS_DECIMAL, 38L),
// BIT("BIT", Types.BIT, Boolean.class, SQLServerType.IS_DECIMAL, 1L),
// SMALLINT("SMALLINT", Types.SMALLINT, Integer.class, SQLServerType.IS_DECIMAL, 5L),
// SMALLMONEY("SMALLMONEY", microsoft.sql.Types.SMALLMONEY, Float.class, SQLServerType.IS_DECIMAL, 6L),
// INT("INT", Types.INTEGER, Integer.class, SQLServerType.IS_DECIMAL, 10L),
// TINYINT("TINYINT", Types.TINYINT, Integer.class, SQLServerType.IS_DECIMAL, 3L),
// MONEY("MONEY", microsoft.sql.Types.MONEY, Double.class, SQLServerType.IS_DECIMAL, 15L),
//
// // SQL Server 将 n 视为下列两个可能值之一 。 如果 1<=n<=24，将 n 视为 24 。 如果 25<=n<=53，将 n 视为 53 。
// // SQL Server float[(n)] 数据类型从 1 到 53 之间的所有 n 值均符合 ISO 标准 。 double precision 的同义词是 float(53) 。
// FLOAT("FLOAT", Types.FLOAT, Float.class, SQLServerType.IS_DECIMAL, 12L),
// REAL("REAL", Types.REAL, Float.class, SQLServerType.IS_DECIMAL, 24L),
//
// DATE("DATE", Types.DATE, Date.class, SQLServerType.IS_NOT_DECIMAL, 10L),
// DATETIME_OFFSET("DATETIME_OFFSET", microsoft.sql.Types.DATETIMEOFFSET, Date.class, SQLServerType.IS_NOT_DECIMAL, 34L),
// DATETIME2("DATETIME2", Types.TIMESTAMP, Date.class, SQLServerType.IS_NOT_DECIMAL, 27L),
// SMALLDATETIME("SMALLDATETIME", microsoft.sql.Types.SMALLDATETIME, Date.class, SQLServerType.IS_NOT_DECIMAL, 19L),
// DATETIME("DATETIME", microsoft.sql.Types.DATETIME, Date.class, SQLServerType.IS_NOT_DECIMAL, 23L),
// TIME("TIME", Types.TIME, Time.class, SQLServerType.IS_NOT_DECIMAL, 16L),
//
// CHAR("CHAR", Types.CHAR, String.class, SQLServerType.IS_NOT_DECIMAL, 8000L),
// VARCHAR("VARCHAR", Types.VARCHAR, String.class, SQLServerType.IS_NOT_DECIMAL, 8000L),
// VARCHAR_MAX("VARCHAR(MAX)", Types.VARCHAR, String.class, SQLServerType.IS_NOT_DECIMAL, 8000L),
// TEXT("TEXT", Types.LONGVARCHAR, String.class, SQLServerType.IS_NOT_DECIMAL, 2147483647L),
//
// NCHAR("NCHAR", Types.NCHAR, String.class, SQLServerType.IS_NOT_DECIMAL, 4000L),
// NVARCHAR("NVARCHAR", Types.NVARCHAR, String.class, SQLServerType.IS_NOT_DECIMAL, 4000L),
// NVARCHAR_MAX("NVARCHAR(MAX)", Types.NVARCHAR, String.class, SQLServerType.IS_NOT_DECIMAL, 4000L),
// NTEXT("NTEXT", Types.LONGNVARCHAR, String.class, SQLServerType.IS_NOT_DECIMAL, 1073741823L),
//
// BINARY("BINARY", Types.BINARY, null, SQLServerType.IS_NOT_DECIMAL, 8000L),
// VARBINARY("VARBINARY", Types.VARBINARY, null, SQLServerType.IS_NOT_DECIMAL, 8000L),
// VARBINARY_MAX("VARBINARY(MAX)", Types.VARBINARY, null, SQLServerType.IS_NOT_DECIMAL, 8000L),
// IMAGE("IMAGE", Types.LONGVARBINARY, null, SQLServerType.IS_NOT_DECIMAL, 2147483647L),
//
// CURSOR("CURSOR", Types.REF_CURSOR, null, SQLServerType.IS_NOT_DECIMAL, 0L),
// // https://docs.microsoft.com/zh-cn/sql/t-sql/data-types/rowversion-transact-sql?view=sql-server-ver15
// ROW_VERSION("ROWVERSION", Types.INTEGER, Integer.class, SQLServerType.IS_DECIMAL, 8L),
// HIERARCHY_ID("HIERARCHYID", Types.OTHER, null, SQLServerType.IS_NOT_DECIMAL, 0L),
// UNIQUE_IDENTIFIER("UNIQUEIDENTIFIER", microsoft.sql.Types.GUID, String.class, SQLServerType.IS_NOT_DECIMAL, 16L),
// // 类型为 sql_variant 的列可能包含不同数据类型的行 。 例如，定义为 sql_variant 的列可以存储 int、binary 和 char 类型的值 。
// // https://docs.microsoft.com/zh-cn/sql/t-sql/data-types/sql-variant-transact-sql?view=sql-server-ver15
// SQL_VARIANT("SQL_VARIANT", microsoft.sql.Types.SQL_VARIANT, String.class, SQLServerType.IS_NOT_DECIMAL, 8016L),
// XML("XML", Types.SQLXML, String.class, SQLServerType.IS_NOT_DECIMAL, 2147483647L),
// GEOMETRY("GEOMETRY", microsoft.sql.Types.GEOMETRY, null, SQLServerType.IS_NOT_DECIMAL, 0L),
// GEOGRAPHY("GEOGRAPHY", microsoft.sql.Types.GEOGRAPHY, null, SQLServerType.IS_NOT_DECIMAL, 0L),
// // TABLE("TABLE", Types.OTHER, null, SQLServerType.IS_NOT_DECIMAL, 0L),
//
// NULL("NULL", Types.NULL, Object.class, SQLServerType.IS_NOT_DECIMAL, 0L),
// UNKNOWN("UNKNOWN", Types.OTHER, null, SQLServerType.IS_NOT_DECIMAL, 65535L)
// ;
//
// private final String name;
// protected int jdbcType;
// protected final Class<?> javaClass;
// private final boolean isDecimal;
// private final Long precision;
//
// SQLServerType(String sqlServerTypeName, int jdbcType, Class<?> javaClass, boolean isDec, Long precision){
//     this.name = sqlServerTypeName;
//     this.jdbcType = jdbcType;
//     this.javaClass = javaClass;
//     this.isDecimal = isDec;
//     this.precision = precision;
// }
//
//
//
// public static SQLServerType getByName(String sqlServerTypeName) {
//     for(SQLServerType sqlServerType: SQLServerType.values()){
//         if(sqlServerTypeName.equalsIgnoreCase(sqlServerType.name)){
//             return sqlServerType;
//         }
//     }
//     return SQLServerType.UNKNOWN;
// }
//
// public static SQLServerType getByJdbcType(int jdbcType) {
//     switch (jdbcType) {
//         case Types.BIGINT:
//             return BIGINT;
//         case Types.BINARY:
//             return BINARY;
//         case Types.BIT:
//             return BIT;
//         case Types.BOOLEAN:
//             return BIT;
//         case Types.CHAR:
//         case Types.NCHAR: // TODO check that it's correct
//             return CHAR;
//         case Types.DATE:
//             return DATE;
//         case Types.DECIMAL:
//         case Types.NUMERIC:
//             return DECIMAL;
//         case Types.DOUBLE:
//         case Types.FLOAT:
//         case Types.REAL:
//             return FLOAT;
//         case Types.INTEGER:
//             return INT;
//         case Types.LONGVARBINARY:
//         case Types.BLOB: // TODO check that it's correct
//         case Types.JAVA_OBJECT: // TODO check that it's correct
//             return IMAGE;
//         case Types.LONGVARCHAR:
//         case Types.LONGNVARCHAR: // TODO check that it's correct
//         case Types.CLOB: // TODO check that it's correct
//         case Types.NCLOB: // TODO check that it's correct
//             return TEXT;
//         case Types.NULL:
//             return NULL;
//         case Types.SMALLINT:
//             return SMALLINT;
//         case Types.TIME:
//         case Types.TIME_WITH_TIMEZONE:
//         case Types.TIMESTAMP_WITH_TIMEZONE:
//             return TIME;
//         case Types.TIMESTAMP:
//             return DATETIME2;
//         case Types.TINYINT:
//             return TINYINT;
//         case Types.VARBINARY:
//             return VARBINARY;
//         case Types.VARCHAR:
//         case Types.NVARCHAR: // TODO check that it's correct
//         case Types.DATALINK: // TODO check that it's correct
//         case Types.SQLXML: // TODO check that it's correct
//             return VARCHAR;
//
//         case Types.REF_CURSOR:
//             return CURSOR;
//
//             // TODO check next types
//         case Types.ARRAY:
//         case Types.DISTINCT:
//         case Types.OTHER:
//         case Types.REF:
//         case Types.ROWID:
//         case Types.STRUCT:
//
//         default:
//             return UNKNOWN;
//     }
// }
//
// /**
//  * 日期类型
//  */
// public static final SQLServerType[] DATE_TYPE_ARRAY = {
//         DATE, DATETIME_OFFSET, DATETIME2, SMALLDATETIME, DATETIME, TIME
// };
//
// /**
//  * 双精度的类型
//  */
// public static final SQLServerType[] SCALE_TYPE_ARRAY = {
//         NUMERIC, DECIMAL, SMALLMONEY, MONEY, FLOAT, REAL,
// };
//
// private static final boolean IS_DECIMAL = true;
// private static final boolean IS_NOT_DECIMAL = false;
//
// @Override
// public String getName() {
//     return this.name;
// }
//
// @Override
// public String getVendor() {
//     return null;
// }
//
// @Override
// public Integer getVendorTypeNumber() {
//     return null;
// }
//
// public int getJdbcType() {
//     return jdbcType;
// }
//
// public boolean isDecimal() {
//     return isDecimal;
// }
//
// public Long getPrecision() {
//     return precision;
// }


// }

public enum SQLServerJDBCType {
    UNKNOWN(Category.UNKNOWN, 999, Object.class.getName()),
    ARRAY(Category.UNKNOWN, java.sql.Types.ARRAY, Object.class.getName()),
    BIGINT(Category.NUMERIC, java.sql.Types.BIGINT, Long.class.getName()),
    BINARY(Category.BINARY, java.sql.Types.BINARY, "[B"),
    BIT(Category.NUMERIC, java.sql.Types.BIT, Boolean.class.getName()),
    BLOB(Category.BLOB, java.sql.Types.BLOB, java.sql.Blob.class.getName()),
    BOOLEAN(Category.NUMERIC, java.sql.Types.BOOLEAN, Boolean.class.getName()),
    CHAR(Category.CHARACTER, java.sql.Types.CHAR, String.class.getName()),
    CLOB(Category.CLOB, java.sql.Types.CLOB, java.sql.Clob.class.getName()),
    DATALINK(Category.UNKNOWN, java.sql.Types.DATALINK, Object.class.getName()),
    DATE(Category.DATE, java.sql.Types.DATE, java.sql.Date.class.getName()),
    DATETIMEOFFSET(Category.DATETIMEOFFSET, microsoft.sql.Types.DATETIMEOFFSET, microsoft.sql.DateTimeOffset.class.getName()),
    DECIMAL(Category.NUMERIC, java.sql.Types.DECIMAL, BigDecimal.class.getName()),
    DISTINCT(Category.UNKNOWN, java.sql.Types.DISTINCT, Object.class.getName()),
    DOUBLE(Category.NUMERIC, java.sql.Types.DOUBLE, Double.class.getName()),
    FLOAT(Category.NUMERIC, java.sql.Types.FLOAT, Double.class.getName()),
    INTEGER(Category.NUMERIC, java.sql.Types.INTEGER, Integer.class.getName()),
    JAVA_OBJECT(Category.UNKNOWN, java.sql.Types.JAVA_OBJECT, Object.class.getName()),
    LONGNVARCHAR(Category.LONG_NCHARACTER, -16, String.class.getName()),
    LONGVARBINARY(Category.LONG_BINARY, java.sql.Types.LONGVARBINARY, "[B"),
    LONGVARCHAR(Category.LONG_CHARACTER, java.sql.Types.LONGVARCHAR, String.class.getName()),
    NCHAR(Category.NCHARACTER, -15, String.class.getName()),
    NCLOB(Category.NCLOB, 2011, java.sql.NClob.class.getName()),
    NULL(Category.UNKNOWN, java.sql.Types.NULL, Object.class.getName()),
    NUMERIC(Category.NUMERIC, java.sql.Types.NUMERIC, BigDecimal.class.getName()),
    NVARCHAR(Category.NCHARACTER, -9, String.class.getName()),
    OTHER(Category.UNKNOWN, java.sql.Types.OTHER, Object.class.getName()),
    REAL(Category.NUMERIC, java.sql.Types.REAL, Float.class.getName()),
    REF(Category.UNKNOWN, java.sql.Types.REF, Object.class.getName()),
    ROWID(Category.UNKNOWN, -8, Object.class.getName()),
    SMALLINT(Category.NUMERIC, java.sql.Types.SMALLINT, Short.class.getName()),
    SQLXML(Category.SQLXML, 2009, Object.class.getName()),
    STRUCT(Category.UNKNOWN, java.sql.Types.STRUCT, Object.class.getName()),
    TIME(Category.TIME, java.sql.Types.TIME, java.sql.Time.class.getName()),
    TIME_WITH_TIMEZONE(Category.TIME_WITH_TIMEZONE, 2013, java.time.OffsetTime.class.getName()),
    TIMESTAMP(Category.TIMESTAMP, java.sql.Types.TIMESTAMP, java.sql.Timestamp.class.getName()),
    TIMESTAMP_WITH_TIMEZONE(Category.TIMESTAMP_WITH_TIMEZONE, 2014, java.time.OffsetDateTime.class.getName()),
    TINYINT(Category.NUMERIC, java.sql.Types.TINYINT, Short.class.getName()),
    VARBINARY(Category.BINARY, java.sql.Types.VARBINARY, "[B"),
    VARCHAR(Category.CHARACTER, java.sql.Types.VARCHAR, String.class.getName()),
    MONEY(Category.NUMERIC, microsoft.sql.Types.MONEY, BigDecimal.class.getName()),
    SMALLMONEY(Category.NUMERIC, microsoft.sql.Types.SMALLMONEY, BigDecimal.class.getName()),
    TVP(Category.TVP, microsoft.sql.Types.STRUCTURED, Object.class.getName()),
    DATETIME(Category.TIMESTAMP, microsoft.sql.Types.DATETIME, java.sql.Timestamp.class.getName()),
    SMALLDATETIME(Category.TIMESTAMP, microsoft.sql.Types.SMALLDATETIME, java.sql.Timestamp.class.getName()),
    GUID(Category.CHARACTER, microsoft.sql.Types.GUID, String.class.getName()),
    SQL_VARIANT(Category.SQL_VARIANT, microsoft.sql.Types.SQL_VARIANT, Object.class.getName()),
    GEOMETRY(Category.GEOMETRY, microsoft.sql.Types.GEOMETRY, Object.class.getName()),
    GEOGRAPHY(Category.GEOGRAPHY, microsoft.sql.Types.GEOGRAPHY, Object.class.getName()),
    LOCALDATETIME(Category.TIMESTAMP, java.sql.Types.TIMESTAMP, LocalDateTime.class.getName());

    final Category category;
    private final int intValue;
    private final String className;
    private static final SQLServerJDBCType[] VALUES = values();

    final String className() {
        return className;
    }

    SQLServerJDBCType(Category category, int intValue, String className) {
        this.category = category;
        this.intValue = intValue;
        this.className = className;
    }

    public int getIntValue() {
        return intValue;
    }

    enum Category {
        CHARACTER,
        LONG_CHARACTER,
        CLOB,
        NCHARACTER,
        LONG_NCHARACTER,
        NCLOB,
        BINARY,
        LONG_BINARY,
        BLOB,
        NUMERIC,
        DATE,
        TIME,
        TIMESTAMP,
        TIME_WITH_TIMEZONE,
        TIMESTAMP_WITH_TIMEZONE,
        DATETIMEOFFSET,
        SQLXML,
        UNKNOWN,
        TVP,
        GUID,
        SQL_VARIANT,
        GEOMETRY,
        GEOGRAPHY;

        private static final Category[] VALUES = values();
    }
}
