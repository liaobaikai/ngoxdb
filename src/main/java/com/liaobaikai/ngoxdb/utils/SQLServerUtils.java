package com.liaobaikai.ngoxdb.utils;

/**
 * @author baikai.liao
 * @Time 2021-01-23 15:30:13
 */
public class SQLServerUtils {

    // /**
    //  * 处理columnType的问题，判断哪些格式需要加上长度，精度，双精度，(MAX)信息
    //  *
    //  * @param sqlServerType
    //  * @param columnInfo
    //  */
    // public static void formatColumnType(SQLServerType sqlServerType, ColumnInfo columnInfo) {
    //
    //     // 没有精度不用增加
    //     if (columnInfo.getPrecision() == null) {
    //         columnInfo.setColumnType(columnInfo.getDataType());
    //     } else if (columnInfo.getPrecision() == -1) {
    //         if (sqlServerType.equals(SQLServerType.XML)) {
    //             columnInfo.setColumnType(columnInfo.getDataType());
    //         } else {
    //             columnInfo.setColumnType(String.format("%s(max)", columnInfo.getDataType()));
    //         }
    //         columnInfo.setCharMaxLength((long) SQLServerType.MAX_VARTYPE_MAX_CHARS);
    //         columnInfo.setCharBytesMaxLength((long) SQLServerType.MAX_VARTYPE_MAX_BYTES);
    //     } else {
    //         if (ArrayUtils.contains(new SQLServerType[]{
    //                 SQLServerType.BIGINT,
    //                 SQLServerType.BIT,
    //                 SQLServerType.DATE,
    //                 SQLServerType.DATETIME,
    //                 SQLServerType.FLOAT,
    //                 SQLServerType.GEOGRAPHY,
    //                 SQLServerType.GEOMETRY,
    //                 // SQLServerType.HIERARCHYID,
    //                 SQLServerType.IMAGE,
    //                 SQLServerType.INTEGER,
    //                 SQLServerType.MONEY,
    //                 SQLServerType.NTEXT,
    //                 SQLServerType.REAL,
    //                 SQLServerType.SMALLDATETIME,
    //                 SQLServerType.SMALLINT,
    //                 SQLServerType.SMALLMONEY,
    //                 SQLServerType.TEXT,
    //                 SQLServerType.TIMESTAMP,
    //                 SQLServerType.TINYINT,
    //                 // SQLServerType.XML,
    //         }, sqlServerType)) {
    //             tableColumnInfo.setColumnType(tableColumnInfo.getDataType());
    //         } else if (sqlServerType.equals(SQLServerType.DECIMAL) || sqlServerType.equals(SQLServerType.NUMERIC)) {
    //             tableColumnInfo.setColumnType(String.format("%s(%s, %s)", tableColumnInfo.getDataType(), tableColumnInfo.getPrecision(), tableColumnInfo.getScale()));
    //             tableColumnInfo.setNumberPrecision(tableColumnInfo.getPrecision());
    //             tableColumnInfo.setNumberScale(tableColumnInfo.getScale());
    //         } else {
    //
    //             if (sqlServerType.equals(SQLServerType.DATETIME2)
    //                     || sqlServerType.equals(SQLServerType.DATETIMEOFFSET)
    //                     || sqlServerType.equals(SQLServerType.TIME)) {
    //                 tableColumnInfo.setColumnType(String.format("%s(%s)", tableColumnInfo.getDataType(), tableColumnInfo.getScale()));
    //                 tableColumnInfo.setDatetimePrecision(tableColumnInfo.getScale());
    //             } else {
    //                 tableColumnInfo.setCharMaxLength((long) tableColumnInfo.getPrecision());
    //                 tableColumnInfo.setCharBytesMaxLength((long) tableColumnInfo.getPrecision());
    //                 tableColumnInfo.setColumnType(String.format("%s(%s)", tableColumnInfo.getDataType(), tableColumnInfo.getPrecision()));
    //             }
    //         }
    //
    //         if (ArrayUtils.contains(new SQLServerType[]{
    //                 SQLServerType.BIGINT,
    //                 SQLServerType.BIT,
    //                 SQLServerType.FLOAT,
    //                 SQLServerType.INTEGER,
    //                 SQLServerType.MONEY,
    //                 SQLServerType.REAL,
    //                 SQLServerType.SMALLINT,
    //                 SQLServerType.SMALLMONEY,
    //                 SQLServerType.TINYINT}, sqlServerType)) {
    //             tableColumnInfo.setNumberPrecision(tableColumnInfo.getPrecision());
    //             tableColumnInfo.setNumberScale(tableColumnInfo.getScale());
    //         }
    //
    //     }
    //
    // }
    //
    // /**
    //  * 通过SQLServerType转成sqlserver可识别的类型
    //  *
    //  * @param sqlServerType
    //  * @param tableColumnInfo
    //  */
    // public static void buildColumnType(SQLServerType sqlServerType, TableColumnInfo tableColumnInfo) {
    //
    //     final String dataType = sqlServerType.getName();
    //
    //     // 字符串
    //     if (tableColumnInfo.getCharMaxLength() != null
    //             && tableColumnInfo.getCharMaxLength() != null) {
    //         if (tableColumnInfo.getCharMaxLength() > SQLServerType.SHORT_VARTYPE_MAX_CHARS) {
    //             // 大的字符串
    //             if (sqlServerType.equals(SQLServerType.XML)) {
    //                 tableColumnInfo.setColumnType(dataType);
    //             } else {
    //                 tableColumnInfo.setColumnType(String.format("%s(max)", dataType));
    //             }
    //         } else {
    //             tableColumnInfo.setColumnType(String.format("%s(%s)", dataType, tableColumnInfo.getCharMaxLength()));
    //         }
    //         return;
    //     }
    //
    //     // 数组
    //     if (sqlServerType.equals(SQLServerType.DECIMAL) || sqlServerType.equals(SQLServerType.NUMERIC)) {
    //         // 数字
    //         if (tableColumnInfo.getNumberScale() > 0) {
    //             tableColumnInfo.setColumnType(String.format("%s(%s)", dataType, tableColumnInfo.getNumberPrecision()));
    //         } else {
    //             tableColumnInfo.setColumnType(String.format("%s(%s, %s)", dataType, tableColumnInfo.getNumberPrecision(), tableColumnInfo.getNumberScale()));
    //         }
    //
    //         return;
    //     }
    //
    //     // 日期
    //     if ((sqlServerType.equals(SQLServerType.DATETIME2) || sqlServerType.equals(SQLServerType.DATETIMEOFFSET) || sqlServerType.equals(SQLServerType.TIME))
    //             && tableColumnInfo.getDatetimePrecision() > 0) {
    //         tableColumnInfo.setColumnType(String.format("%s(%s)", dataType, tableColumnInfo.getDatetimePrecision()));
    //         return;
    //     }
    //
    //     tableColumnInfo.setColumnType(dataType);
    //
    // }
}
