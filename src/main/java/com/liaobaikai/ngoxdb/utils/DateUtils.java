package com.liaobaikai.ngoxdb.utils;

import com.liaobaikai.ngoxdb.enums.DatabaseVendorEnum;

import java.sql.Types;

/**
 * @author baikai.liao
 * @Time 2021-01-23 16:48:57
 */
public class DateUtils {

    /**
     * 转换默认值
     *
     * @param masterDataBaseVendor {@link com.liaobaikai.ngoxdb.enums.DatabaseVendorEnum vendor}
     * @param slaveDataBaseVendor  {@link com.liaobaikai.ngoxdb.enums.DatabaseVendorEnum vendor}
     * @param defaultValue         默认值
     * @param jdbcType             jdbc类型
     * @return
     */
    public static String convert(String masterDataBaseVendor,
                                 String slaveDataBaseVendor,
                                 String defaultValue,
                                 int jdbcType) {

        // 厂家相同
        if (masterDataBaseVendor.equals(slaveDataBaseVendor)
                || ((DatabaseVendorEnum.isMySQL(masterDataBaseVendor) || DatabaseVendorEnum.isMariadb(masterDataBaseVendor)) && (DatabaseVendorEnum.isMySQL(slaveDataBaseVendor) || DatabaseVendorEnum.isMariadb(slaveDataBaseVendor)))) {
            return defaultValue;
        }

        if (!(jdbcType == Types.DATE
                || jdbcType == Types.TIMESTAMP
                || jdbcType == Types.TIME
                || jdbcType == Types.TIME_WITH_TIMEZONE
                || jdbcType == Types.TIMESTAMP_WITH_TIMEZONE
                || jdbcType == microsoft.sql.Types.DATETIMEOFFSET
                || jdbcType == microsoft.sql.Types.SMALLDATETIME)) {
            return defaultValue;
        }

        // 源数据库是mysql
        if ((DatabaseVendorEnum.MYSQL.getVendor().equals(masterDataBaseVendor) || DatabaseVendorEnum.MARIADB.getVendor().equals(masterDataBaseVendor))
                && DatabaseVendorEnum.SQLSERVER.getVendor().equals(slaveDataBaseVendor)) {
            if ("CURRENT_TIMESTAMP".equalsIgnoreCase(defaultValue)) {
                return "getdate()";
            } else if ("current_timestamp()".equalsIgnoreCase(defaultValue)) {
                return "getdate()";
            } else {
                // ....
            }
        }

        return defaultValue;
    }

}
