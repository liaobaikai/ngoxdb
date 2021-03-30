package com.liaobaikai.ngoxdb.core.constant;

import java.util.HashMap;
import java.util.Map;

/**
 * jdbc数据类型
 * {@link java.sql.Types}
 * {@link microsoft.sql.Types}
 *
 * @author baikai.liao
 * @Time 2021-03-11 23:44:41
 */
public class JdbcDataType {

    public final static int BIT = -7;
    public final static int TINYINT = -6;
    public final static int SMALLINT = 5;
    public final static int INTEGER = 4;
    public final static int BIGINT = -5;
    public final static int FLOAT = 6;
    public final static int REAL = 7;
    public final static int DOUBLE = 8;
    public final static int NUMERIC = 2;
    public final static int DECIMAL = 3;
    public final static int CHAR = 1;
    public final static int VARCHAR = 12;
    public final static int LONGVARCHAR = -1;
    public final static int DATE = 91;
    public final static int TIME = 92;
    public final static int TIMESTAMP = 93;
    public final static int BINARY = -2;
    public final static int VARBINARY = -3;
    public final static int LONGVARBINARY = -4;
    public final static int NULL = 0;
    public final static int OTHER = 1111;
    public final static int JAVA_OBJECT = 2000;
    public final static int DISTINCT = 2001;
    public final static int STRUCT = 2002;
    public final static int ARRAY = 2003;
    public final static int BLOB = 2004;
    public final static int CLOB = 2005;
    public final static int REF = 2006;
    public final static int DATALINK = 70;
    public final static int BOOLEAN = 16;
    public final static int ROWID = -8;
    public static final int NCHAR = -15;
    public static final int NVARCHAR = -9;
    public static final int LONGNVARCHAR = -16;
    public static final int NCLOB = 2011;
    public static final int SQLXML = 2009;
    public static final int REF_CURSOR = 2012;
    public static final int TIME_WITH_TIMEZONE = 2013;
    public static final int TIMESTAMP_WITH_TIMEZONE = 2014;


    // --------------------- sqlserver ---------------------
    public static final int DATETIMEOFFSET = -155;
    public static final int STRUCTURED = -153;
    public static final int DATETIME = -151;
    public static final int SMALLDATETIME = -150;
    public static final int MONEY = -148;
    public static final int SMALLMONEY = -146;
    public static final int GUID = -145;
    public static final int SQL_VARIANT = -156;
    public static final int GEOMETRY = -157;
    public static final int GEOGRAPHY = -158;
    // --------------------- sqlserver ---------------------

    // --------------------- oracle ---------------------
    public static final int JAVA_STRUCT = 2008;
    @Deprecated
    public static final int TIMESTAMPNS = -100;
    public static final int TIMESTAMPTZ = -101;     // alias: TIMESTAMP_WITH_TIMEZONE
    public static final int TIMESTAMPLTZ = -102;    // alias: TIMESTAMP WITH LOCAL TIME ZONE
    public static final int INTERVALYM = -103;
    public static final int INTERVALDS = -104;
    public static final int CURSOR = 120;
    public static final int BFILE = -13;
    public static final int OPAQUE = 2007;
    public static final int PLSQL_INDEX_TABLE = -14;
    public static final int BINARY_FLOAT = 100;
    public static final int BINARY_DOUBLE = 101;
    public static final int NUMBER = 2;
    public static final int RAW = -2;
    public static final int FIXED_CHAR = 999;
    // --------------------- oracle ---------------------

    // --------------------- mysql ---------------------
    //     extension data type, non MySQL standard.
    public static final int JSON = -3000;
    public static final int ENUM = -3001;
    public static final int SET = -3002;
    public static final int MEDIUMBLOB = -3003;
    public static final int MEDIUMTEXT = -3004;
    public static final int LONGBLOB = -3005;
    public static final int LONGTEXT = -3006;
    public static final int YEAR = -3007;
    // --------------------- mysql ---------------------

    public static final Map<Integer, String> JDBC_DATA_TYPE_NAME_BINDER = new HashMap<Integer, String>() {
        {
            put(JdbcDataType.BIT, "bit");
            put(JdbcDataType.TINYINT, "tinyint");
            put(JdbcDataType.SMALLINT, "smallint");
            put(JdbcDataType.INTEGER, "integer");
            put(JdbcDataType.BIGINT, "bigint");
            put(JdbcDataType.FLOAT, "float");
            put(JdbcDataType.REAL, "real");
            put(JdbcDataType.DOUBLE, "double");
            put(JdbcDataType.NUMERIC, "numeric");
            put(JdbcDataType.DECIMAL, "decimal");
            put(JdbcDataType.CHAR, "char");
            put(JdbcDataType.VARCHAR, "varchar");
            put(JdbcDataType.LONGVARCHAR, "longvarchar");
            put(JdbcDataType.DATE, "date");
            put(JdbcDataType.TIME, "time");
            put(JdbcDataType.TIMESTAMP, "timestamp");
            put(JdbcDataType.BINARY, "binary");
            put(JdbcDataType.VARBINARY, "varbinary");
            put(JdbcDataType.LONGVARBINARY, "longvarbinary");
            put(JdbcDataType.NULL, "null");
            put(JdbcDataType.OTHER, "other");
            put(JdbcDataType.JAVA_OBJECT, "java_object");
            put(JdbcDataType.DISTINCT, "distinct");
            put(JdbcDataType.STRUCT, "struct");
            put(JdbcDataType.ARRAY, "array");
            put(JdbcDataType.BLOB, "blob");
            put(JdbcDataType.CLOB, "clob");
            put(JdbcDataType.REF, "ref");
            put(JdbcDataType.DATALINK, "datalink");
            put(JdbcDataType.BOOLEAN, "boolean");
            put(JdbcDataType.ROWID, "rowid");
            put(JdbcDataType.NCHAR, "nchar");
            put(JdbcDataType.NVARCHAR, "nvarchar");
            put(JdbcDataType.LONGNVARCHAR, "longnvarchar");
            put(JdbcDataType.NCLOB, "nclob");
            put(JdbcDataType.SQLXML, "sqlxml");
            put(JdbcDataType.REF_CURSOR, "ref_cursor");
            put(JdbcDataType.TIME_WITH_TIMEZONE, "time_with_timezone");
            put(JdbcDataType.TIMESTAMP_WITH_TIMEZONE, "timestamp_with_timezone");

            // --------------------- sqlserver ---------------------
            put(JdbcDataType.DATETIMEOFFSET, "datetimeoffset");
            put(JdbcDataType.STRUCTURED, "structured");
            put(JdbcDataType.DATETIME, "datetime");
            put(JdbcDataType.SMALLDATETIME, "smalldatetime");
            put(JdbcDataType.MONEY, "money");
            put(JdbcDataType.SMALLMONEY, "smallmoney");
            put(JdbcDataType.GUID, "guid");
            put(JdbcDataType.SQL_VARIANT, "sql_variant");
            put(JdbcDataType.GEOMETRY, "geometry");
            put(JdbcDataType.GEOGRAPHY, "geography");
            // --------------------- sqlserver ---------------------

            // --------------------- oracle ---------------------
            put(JdbcDataType.JAVA_STRUCT, "java_struct");
            // @Deprecated
            // put(JdbcDataType.TIMESTAMPNS,"timestampns");
            put(JdbcDataType.TIMESTAMPTZ, "timestamptz");
            put(JdbcDataType.TIMESTAMPLTZ, "timestampltz");
            put(JdbcDataType.INTERVALYM, "intervalym");
            put(JdbcDataType.INTERVALDS, "intervalds");
            put(JdbcDataType.CURSOR, "cursor");
            put(JdbcDataType.BFILE, "bfile");
            put(JdbcDataType.OPAQUE, "opaque");
            put(JdbcDataType.PLSQL_INDEX_TABLE, "plsql_index_table");
            put(JdbcDataType.BINARY_FLOAT, "binary_float");
            put(JdbcDataType.BINARY_DOUBLE, "binary_double");
            // put(JdbcDataType.NUMBER, "number");
            // put(JdbcDataType.RAW, "raw");
            put(JdbcDataType.FIXED_CHAR, "fixed_char");
            // --------------------- oracle ---------------------

            // // --------------------- mysql ---------------------
            // put(JdbcDataType.JSON, "json");
            // put(JdbcDataType.ENUM, "enum");
            // put(JdbcDataType.SET, "set");
            // put(JdbcDataType.MEDIUMBLOB, "mediumblob");
            // put(JdbcDataType.MEDIUMTEXT, "mediumtext");
            // put(JdbcDataType.LONGBLOB, "longblob");
            // put(JdbcDataType.LONGTEXT, "longtext");
            // // --------------------- mysql ---------------------
        }
    };

    // cat JdbcDataType | sed 's/public static final int / case JdbcDataType./' | sed 's/public final static int / case JdbcDataType./' | sed 's/=.*$/: break;/'
}
