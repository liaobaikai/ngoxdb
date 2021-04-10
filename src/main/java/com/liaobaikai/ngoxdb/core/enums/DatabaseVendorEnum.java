package com.liaobaikai.ngoxdb.core.enums;

import lombok.Getter;

/**
 * 数据库厂家
 *
 * @author baikai.liao
 * @Time 2021-01-29 23:44:13
 */
@Getter
public enum DatabaseVendorEnum {

    SQLSERVER("sqlserver", "com.microsoft.sqlserver.jdbc.SQLServerDriver", 1433),
    MYSQL("mysql", "com.mysql.cj.jdbc.Driver", 3306),
    MARIADB("mariadb", "org.mariadb.jdbc.Driver", 3306),
    POSTGRESQL("postgresql", "org.postgresql.Driver", 5432),
    ORACLE("oracle", "oracle.jdbc.OracleDriver", 1521),
    DB2("db2", "com.ibm.db2.jcc.DB2Driver", 5000),
    DM("dm", "dm.jdbc.driver.DmDriver", 5236),  // from: windows版本: dmdbms/drivers/jdbc/USAGE.txt
    MICROSOFT_ACCESS("msaccess", "net.ucanaccess.jdbc.UcanaccessDriver", -1),
    SQLITE("sqlite", "org.sqlite.JDBC", -1),

    ;

    private final String vendor;
    private final String driverClassName;
    private final int defaultPort;

    DatabaseVendorEnum(String vendor, String driverClassName, int defaultPort) {
        this.vendor = vendor;
        this.driverClassName = driverClassName;
        this.defaultPort = defaultPort;

    }

    public static DatabaseVendorEnum getByName(String vendor) {
        for (DatabaseVendorEnum databaseVendor : DatabaseVendorEnum.values()) {
            if (databaseVendor.getVendor().equals(vendor)) {
                return databaseVendor;
            }
        }
        return null;
    }

    public static boolean isMySQL(String vendor) {
        return DatabaseVendorEnum.MYSQL.equals(getByName(vendor));
    }

    public static boolean isMySQLFamily(String vendor) {
        return isMariadb(vendor) || isMySQL(vendor);
    }

    public static boolean isMariadb(String vendor) {
        return DatabaseVendorEnum.MARIADB.equals(getByName(vendor));
    }

    public static boolean isOracle(String vendor) {
        return DatabaseVendorEnum.ORACLE.equals(getByName(vendor));
    }

    public static boolean isSQLServer(String vendor) {
        return DatabaseVendorEnum.SQLSERVER.equals(getByName(vendor));
    }

    public static boolean isPostgreSQL(String vendor) {
        return DatabaseVendorEnum.POSTGRESQL.equals(getByName(vendor));
    }

    public static boolean isMicrosoftAccess(String vendor) {
        return DatabaseVendorEnum.MICROSOFT_ACCESS.equals(getByName(vendor));
    }

    public static boolean isDB2(String vendor) {
        return DatabaseVendorEnum.DB2.equals(getByName(vendor));
    }

    public static boolean isDM(String vendor) {
        return DatabaseVendorEnum.DM.equals(getByName(vendor));
    }

    public static boolean isSameDatabaseVendor(String vendor, String vendor2) {
        return vendor.equals(vendor2) || (isMySQLFamily(vendor) && isMySQLFamily(vendor2));
    }

}
