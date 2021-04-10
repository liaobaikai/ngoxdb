package com.liaobaikai.ngoxdb.core.config;

import com.liaobaikai.ngoxdb.anno.Description;
import com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum;
import com.liaobaikai.ngoxdb.utils.StringUtils;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    // access database默认参数
    private static final LinkedHashMap<String, String> ACCESS_DEFAULT_PARAMS = new LinkedHashMap<String, String>() {
        {
            put("sysSchema", "true");
            put("ignoreCase", "true");
            put("newDatabaseVersion", "V2003");
        }
    };

    /**
     * 配置标识
     */
    @Description(name = "name", label = "配置标识", defaultValue = "slave|master", slaveParam = true, masterParam = true)
    private String name;

    /**
     * 数据库厂家 {@link com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum}
     */
    @Description(name = "database", label = "数据库厂家", slaveParam = true, masterParam = true)
    private String database;

    /**
     * 主机
     */
    @Description(name = "host", label = "主机名", slaveParam = true, masterParam = true)
    private String host;

    /**
     * 端口
     */
    @Description(name = "port", label = "端口", slaveParam = true, masterParam = true)
    private String port;

    /**
     * 数据库名
     */
    @Description(name = "database-name", label = "数据库名", slaveParam = true, masterParam = true)
    private String databaseName;

    /**
     * 驱动类
     */
    @Description(name = "driver-class-name", label = "驱动类", slaveParam = true, masterParam = true)
    private String driverClassName;

    /**
     * 用户名
     */
    @Description(name = "username", label = "用户名", slaveParam = true, masterParam = true)
    private String username;

    /**
     * 密码
     */
    @Description(name = "password", label = "密码", slaveParam = true, masterParam = true)
    private String password;

    /**
     * 其他参数信息
     */
    @Description(name = "params", label = "其他参数信息", slaveParam = true, masterParam = true)
    private String params;

    /**
     * 本地文件
     */
    @Description(name = "local-file", label = "本地文件", applyOn = {DatabaseVendorEnum.MICROSOFT_ACCESS, DatabaseVendorEnum.SQLITE}, slaveParam = true, masterParam = true)
    private String localFile;

    /**
     * 生成的数据库版本
     */
    @Description(name = "new-database-version", label = "生成的数据库版本", defaultValue = "V2007", applyOn = {DatabaseVendorEnum.MICROSOFT_ACCESS}, slaveParam = true, masterParam = true)
    private String newDatabaseVersion = "V2007";

    /**
     * jdbc url
     */
    @Description(name = "jdbc-url", label = "jdbc url", defaultValue = "根据其他配置参数自动生成", slaveParam = true, masterParam = true)
    private String jdbcUrl;

    /**
     * 表存在是否替换
     */
    @Description(name = "replace-table", label = "表存在时是否替换(false: 跳过, true: 替换)?", defaultValue = "false", slaveParam = true)
    private boolean replaceTable;

    /**
     * 表数据存在是否截断
     */
    @Description(name = "truncate-table", label = "表数据存在时是否截断(false: 跳过, true: 替换)?", defaultValue = "false", slaveParam = true)
    private boolean truncateTable;

    /**
     * 创建表的参数
     */
    @Description(name = "create-table-params", label = "创建表的参数", slaveParam = true)
    private String createTableParams;

    /**
     * 协议
     */
    @Description(name = "protocol", label = "协议: tcp, tcps...等等", applyOn = {DatabaseVendorEnum.ORACLE}, slaveParam = true, masterParam = true)
    private String protocol;

    /**
     * 多个 host1[,host2,host3][:port1][,host4:port2]
     */
    @Description(name = "servers", label = "多个主机名，如: host1[,host2,host3][:port1][,host4:port2]", applyOn = {DatabaseVendorEnum.ORACLE}, slaveParam = true, masterParam = true)
    private String servers;

    /**
     * 自动生成名称，主要针对 索引，约束，外键等。
     */
    @Description(name = "generate-name", label = "自动生成名称，主要针对索引(false: 默认, true: 自动生成)。", defaultValue = "false", slaveParam = true)
    private boolean generateName;

    /**
     * 每页大小，默认256
     */
    @Description(name = "page-size", label = "批量导出导入的数量", defaultValue = "256", masterParam = true)
    private int pageSize = 256;

    /**
     * 需要处理的表名，用逗号隔开
     */
    @Description(name = "tables", label = "需要处理的表名，用逗号隔开", masterParam = true)
    private String tables;

    /**
     * 重新映射表名，如：a:a1,b:b2
     */
    @Description(name = "remap-table", label = "重新映射表名，如：a:a1,b:b2", slaveParam = true)
    private String remapTable = "";

    /**
     * 重新映射表列名，如：a.c1:a.c2,b:b2
     */
    @Description(name = "remap-column", label = "重新映射表的列名，如：a1.c1:a1.a1,b2.c1:b2.c2，如果存在表名映射，指定的表名应该是映射后的表名", slaveParam = true)
    private String remapColumn = "";

    @Description(name = "parallel-workers", label = "并行工作的线程数，默认为可用的核心线程数的2倍，主库(收集表信息、导出数据)；从库(导入数据)", slaveParam = true, masterParam = true)
    private int parallelWorkers = Runtime.getRuntime().availableProcessors() * 2;

    @Description(name = "thread-pool-size", label = "线程池大小，默认为并行工作的线程数的2倍", slaveParam = true, masterParam = true)
    private int threadPoolSize;

    public DatabaseConfig() {
        this.threadPoolSize = parallelWorkers * 2;
    }

    /**
     * 获取驱动类名
     *
     * @return
     */
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
                ACCESS_DEFAULT_PARAMS.put("newDatabaseVersion", this.newDatabaseVersion);
            }
            for (Map.Entry<String, String> en : ACCESS_DEFAULT_PARAMS.entrySet()) {
                urlBuilder.append(";").append(en.getKey()).append("=").append(en.getValue());
            }
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

        } else if (DatabaseVendorEnum.DM.getDriverClassName().equals(driverClassName)) {
            // 达梦数据库
            if (StringUtils.isEmpty(this.port)) {
                this.port = Integer.valueOf(DatabaseVendorEnum.DM.getDefaultPort()).toString();
            }
            urlBuilder.append(this.database).append("://");

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

    public void setParallelWorkers(int parallelWorkers) {
        int defaultParallelWorkers = this.parallelWorkers;
        this.parallelWorkers = parallelWorkers;
        if(defaultParallelWorkers * 2 == this.threadPoolSize || this.threadPoolSize == 0){
            this.threadPoolSize = parallelWorkers * 2;
        }
    }
}
