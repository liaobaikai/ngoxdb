package com.liaobaikai.ngoxdb.service.impl.ba;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

/**
 * @author baikai.liao
 * @Time 2021-01-27 17:11:31
 */
@Service("MariadbConverter")
@Scope("prototype")
public class MariadbConverter{

    // public MariadbConverter(@Qualifier("masterJdbcTemplate") SimpleJdbcTemplate masterJdbcTemplate,
    //                         @Qualifier("slaveJdbcTemplate") SimpleJdbcTemplate slaveJdbcTemplate) {
    //     super(masterJdbcTemplate, slaveJdbcTemplate);
    // }
}
