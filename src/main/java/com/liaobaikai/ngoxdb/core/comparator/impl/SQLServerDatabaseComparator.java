package com.liaobaikai.ngoxdb.core.comparator.impl;

import com.liaobaikai.ngoxdb.core.comparator.BasicDatabaseComparator;
import com.liaobaikai.ngoxdb.core.converter.DatabaseConverter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

/**
 * @author baikai.liao
 * @Time 2021-03-04 23:07:18
 */
@Slf4j
public class SQLServerDatabaseComparator extends BasicDatabaseComparator {

    public SQLServerDatabaseComparator(DatabaseConverter databaseConverter) {
        super(databaseConverter);
    }

    @Override
    public Logger getLogger() {
        return log;
    }
}
