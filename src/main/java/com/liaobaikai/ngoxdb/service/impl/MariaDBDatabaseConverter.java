package com.liaobaikai.ngoxdb.service.impl;

import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
import com.liaobaikai.ngoxdb.config.DatabaseConfig;
import com.liaobaikai.ngoxdb.enums.DatabaseVendorEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

/**
 * MariaDB 转换器
 * @author baikai.liao
 * @Time 2021-01-28 15:42:06
 */
@Slf4j
@Service
public class MariaDBDatabaseConverter extends MySQLDatabaseConverter {

    public MariaDBDatabaseConverter() {
    }

    public MariaDBDatabaseConverter(JdbcTemplate2 jdbcTemplate,
                                    boolean isMaster,
                                    String masterDatabaseVendor,
                                    DatabaseConfig databaseConfig) {
        super(jdbcTemplate, isMaster, masterDatabaseVendor, databaseConfig);
    }

    @Override
    public String getDatabaseVendor() {
        return DatabaseVendorEnum.MARIADB.getVendor();
    }

}
