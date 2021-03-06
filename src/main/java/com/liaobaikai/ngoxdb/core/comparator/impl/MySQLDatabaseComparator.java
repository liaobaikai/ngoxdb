package com.liaobaikai.ngoxdb.core.comparator.impl;

import com.liaobaikai.ngoxdb.core.comparator.BasicDatabaseComparator;
import com.liaobaikai.ngoxdb.core.converter.DatabaseConverter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

/**
 * MySQL数据库比较器
 *
 * @author baikai.liao
 * @Time 2021-03-04 14:58:11
 */
@Slf4j
public class MySQLDatabaseComparator extends BasicDatabaseComparator {

    public MySQLDatabaseComparator(DatabaseConverter databaseConverter) {
        super(databaseConverter);
    }

    @Override
    public Logger getLogger() {
        return log;
    }

}
