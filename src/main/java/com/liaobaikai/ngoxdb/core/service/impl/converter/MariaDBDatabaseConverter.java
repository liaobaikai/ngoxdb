package com.liaobaikai.ngoxdb.core.service.impl.converter;

import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
import com.liaobaikai.ngoxdb.core.config.DatabaseConfig;
import com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum;
import com.liaobaikai.ngoxdb.core.service.DatabaseComparator;
import com.liaobaikai.ngoxdb.core.service.impl.comparator.MariaDBDatabaseComparator;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * MariaDB 转换器
 *
 * @author baikai.liao
 * @Time 2021-01-28 15:42:06
 */
@Slf4j
@Service
public class MariaDBDatabaseConverter extends MySQLDatabaseConverter {

    private final MariaDBDatabaseComparator databaseComparator;

    public MariaDBDatabaseConverter() {
        this.databaseComparator = null;
    }

    public MariaDBDatabaseConverter(JdbcTemplate2 jdbcTemplate,
                                    boolean isMaster,
                                    String masterDatabaseVendor,
                                    DatabaseConfig databaseConfig) {
        super(jdbcTemplate, isMaster, masterDatabaseVendor, databaseConfig);
        this.databaseComparator = new MariaDBDatabaseComparator(this);
    }

    @Override
    public String getDatabaseVendor() {
        return DatabaseVendorEnum.MARIADB.getVendor();
    }

    @Override
    public DatabaseComparator getComparator() {
        return this.databaseComparator;
    }

    @Override
    public Logger getLogger() {
        return log;
    }
}
