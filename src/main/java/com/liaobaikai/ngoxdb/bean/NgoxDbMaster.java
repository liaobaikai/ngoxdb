package com.liaobaikai.ngoxdb.bean;

import com.liaobaikai.ngoxdb.boot.JdbcTemplate;
import com.liaobaikai.ngoxdb.core.config.DatabaseConfig;
import com.liaobaikai.ngoxdb.core.dialect.DatabaseDialect;
import lombok.Getter;
import lombok.Setter;

/**
 * @author baikai.liao
 * @Time 2021-03-17 17:21:29
 */
@Setter
@Getter
public class NgoxDbMaster {

    /**
     * {@link JdbcTemplate}
     */
    private JdbcTemplate jdbcTemplate;

    /**
     * 数据配置
     */
    private DatabaseConfig databaseConfig;

    /**
     * 方言
     */
    private DatabaseDialect databaseDialect;

    /**
     * 源数据库厂家名称 {@link com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum}
     */
    private String masterDatabaseVendor;

    /**
     * 是否为源数据库
     */
    private boolean isMaster;


}
