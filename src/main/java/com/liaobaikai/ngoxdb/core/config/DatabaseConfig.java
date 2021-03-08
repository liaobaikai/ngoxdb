package com.liaobaikai.ngoxdb.core.config;

import com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum;
import com.liaobaikai.ngoxdb.utils.StringUtils;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * 数据库连接信息
 *
 * @author baikai.liao
 * @Time 2021-01-17 17:03:48
 */
@Setter
@Getter
public class DatabaseConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseConfig.class);

    // MySQL 默认参数
    private static final LinkedHashMap<String, String> MYSQL_DEFAULT_PARAMS = new LinkedHashMap<String, String>() {
        {
            put("useUnicode", "true");
            put("characterEncoding", "utf8");
            put("rewriteBatchedStatements", "true");
            put("zeroDateTimeBehavior", "convertToNull");
            put("allowMultiQueries", "true");
            put("useSSL", "true");      // https://bugs.mysql.com/bug.php?id=21429  Bug #21429 	Unsupported record version Unknown-0.0
            put("serverTimezone", "GMT%2B8");
            put("useInformationSchema", "true");
        }
    };

    // Mariadb 默认参数
    private static final LinkedHashMap<String, String> MARIADB_DEFAULT_PARAMS = new LinkedHashMap<String, String>(MYSQL_DEFAULT_PARAMS) {
        {
            // default
            remove("useInformationSchema");
            // https://bugs.mysql.com/bug.php?id=21429  Bug #21429 	Unsupported record version Unknown-0.0
            remove("useSSL");
        }
    };

    /**
     * 配置标识
     */
    private String name;

    /**
     * 数据库厂家 {@link com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum}
     */
    private String database;

    /**
     * 主机
     */
    private String host;

    /**
     * 端口
     */
    private String port;

    /**
     * 数据库名
     */
    private String databaseName;

    /**
     * 驱动类
     */
    private String driverClassName;

    /**
     * 用户名
     */
    private String username;

    /**
     * 密码
     */
    private String password;

    /**
     * 其他参数信息
     */
    private String params;

    /**
     * 本地文件
     */
    private String localFile;

    /**
     * 生成的数据库版本
     */
    private String newDatabaseVersion = "V2007";

    /**
     * jdbc url
     */
    private String jdbcUrl;

    /**
     * 表存在是否替换
     */
    private boolean replaceTable;

    /**
     * 表数据存在是否截断
     */
    private boolean truncateTable;

    /**
     * 创建表的参数
     */
    private String createTableParams;

    /**
     * 如果表没有主键的话，且改列是自动增长的话，需要将该列设置为主键列
     * 默认为false
     */
    private boolean primaryKeyWithAutoincrementColumns;

    /**
     * 协议
     */
    private String protocol;

    /**
     * 多个 host1[,host2,host3][:port1][,host4:port2]
     */
    private String servers;

    /**
     * 目标的名称转换为小写
     */
    private boolean lowerCaseName;

    /**
     * 自动生成名称，主要针对 索引，约束，外键等。
     */
    private boolean generateName;

    /**
     * 数据库操作控制台日志
     */
    private boolean dbConsoleLog;

    /**
     * 每页大小，默认1000
     */
    private int pageSize = 256;

    /**
     * 是否批量插入
     */
    private boolean batchInsert = true;


    public String getDriverClassName() {

        // 判断数据库是否存在
        boolean exists = false;
        List<String> vendors = new ArrayList<>();
        for (DatabaseVendorEnum dataBaseVendor : DatabaseVendorEnum.values()) {
            if (!exists && dataBaseVendor.getVendor().equalsIgnoreCase(database)) {
                exists = true;
                // 自动设置驱动
                this.driverClassName = dataBaseVendor.getDriverClassName();
            }
            vendors.add(dataBaseVendor.getVendor());
        }

        if (!exists) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("无效的参数：spring.datasource.(master|slave).database={}，可选值为：{}", database, vendors.toString());
            }
        }

        return this.driverClassName;
    }

    /**
     * jdbc url
     *
     * @return url
     */
    public String getUrl() {
        if (StringUtils.isNotEmpty(this.jdbcUrl)) {
            return this.jdbcUrl;
        }

        String driverClassName = this.getDriverClassName();

        StringBuilder urlBuilder = new StringBuilder("jdbc:");
        if (DatabaseVendorEnum.SQLITE.getDriverClassName().equals(driverClassName)) {
            // jdbc:sqlite:/data/test1.db
            urlBuilder.append("sqlite:").append(this.localFile);
            return urlBuilder.toString();
        } else if (DatabaseVendorEnum.SQLSERVER.getDriverClassName().equals(driverClassName)) {
            if (StringUtils.isEmpty(this.port)) {
                this.port = Integer.valueOf(DatabaseVendorEnum.SQLSERVER.getDefaultPort()).toString();
            }
            urlBuilder.append(this.database).append("://").append(this.host).append(":").append(this.port)
                    .append(";databaseName=").append(this.databaseName);
            return urlBuilder.toString();
        } else if (DatabaseVendorEnum.MICROSOFT_ACCESS.getDriverClassName().equals(driverClassName)) {
            // access
            // http://ucanaccess.sourceforge.net/site.html#examples
            urlBuilder.append("ucanaccess://").append(this.localFile);
            if (StringUtils.isNotEmpty(this.newDatabaseVersion)) {
                urlBuilder.append(";newDatabaseVersion=").append(this.newDatabaseVersion);
            }
            urlBuilder.append(";sysSchema=true;ignoreCase=true");
            return urlBuilder.toString();
        } else if (DatabaseVendorEnum.MYSQL.getDriverClassName().equals(driverClassName)
                || DatabaseVendorEnum.MARIADB.getDriverClassName().equals(driverClassName)) {
            if (StringUtils.isEmpty(this.port)) {
                this.port = Integer.valueOf(DatabaseVendorEnum.MYSQL.getDefaultPort()).toString();
            }
            urlBuilder.append(this.database).append("://");
            urlBuilder.append(this.host).append(":").append(this.port).append("/").append(this.databaseName);

            LinkedHashMap<String, String> mysqlParams = new LinkedHashMap<>(
                    DatabaseVendorEnum.MYSQL.getDriverClassName().equals(driverClassName) ? MYSQL_DEFAULT_PARAMS : MARIADB_DEFAULT_PARAMS
            );
            if (this.params != null && this.params.trim().length() > 0) {
                LinkedHashMap<String, String> params = StringUtils.url2Map(this.params);
                mysqlParams.putAll(params);
            }

            urlBuilder.append("?").append(StringUtils.map2Url(mysqlParams));

            return urlBuilder.toString();

        } else if (DatabaseVendorEnum.POSTGRESQL.getDriverClassName().equals(driverClassName)) {
            if (StringUtils.isEmpty(this.port)) {
                this.port = Integer.valueOf(DatabaseVendorEnum.POSTGRESQL.getDefaultPort()).toString();
            }
            urlBuilder.append(this.database).append("://");
        } else if (DatabaseVendorEnum.ORACLE.getDriverClassName().equals(driverClassName)) {
            if (StringUtils.isEmpty(this.port)) {
                this.port = Integer.valueOf(DatabaseVendorEnum.ORACLE.getDefaultPort()).toString();
            }
            // service name

            urlBuilder.append(this.database).append(":thin:@");
            if (StringUtils.isNotEmpty(this.protocol)) {
                urlBuilder.append(this.protocol).append("://");
            } else {
                urlBuilder.append("//");
            }

        } else if (DatabaseVendorEnum.DB2.getDriverClassName().equals(driverClassName)) {
            if (StringUtils.isEmpty(this.port)) {
                this.port = Integer.valueOf(DatabaseVendorEnum.DB2.getDefaultPort()).toString();
            }
            urlBuilder.append(this.database).append(":");
        }

        if (StringUtils.isNotEmpty(this.host)) {
            // host没设置
            // host只针对具体的一个ip/域名
            urlBuilder.append(this.host).append(":").append(this.port);
        } else if (StringUtils.isNotEmpty(this.servers)) {
            // servers=host:port,host2:port
            urlBuilder.append(this.servers);
        }

        urlBuilder.append("/").append(this.databaseName);

        if (StringUtils.isNotEmpty(this.params)) {
            urlBuilder.append("?").append(this.params);
        }

        return urlBuilder.toString();
    }

    // @Override
    // public String toString() {
    //
    //     return JSONObject.toJSONString(this,
    //             SerializerFeature.WRITE_MAP_NULL_FEATURES,
    //             SerializerFeature.PrettyFormat,
    //             SerializerFeature.DisableCircularReferenceDetect);
    //
    // }

    /**
     * 判断是否为有效的配置信息
     *
     * @return boolean
     */
    public boolean isValid() {
        return ((StringUtils.isNotEmpty(this.localFile)) && StringUtils.isNotEmpty(this.database))
                || (StringUtils.isNotEmpty(this.database) && StringUtils.isNotEmpty(this.getUrl())
                && StringUtils.isNotEmpty(this.driverClassName) && StringUtils.isNotEmpty(this.username)
                && StringUtils.isNotEmpty(this.password));
    }
}
