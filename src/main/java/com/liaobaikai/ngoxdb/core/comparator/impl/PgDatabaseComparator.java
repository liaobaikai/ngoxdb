package com.liaobaikai.ngoxdb.core.comparator.impl;

import com.liaobaikai.ngoxdb.core.comparator.BasicDatabaseComparator;
import com.liaobaikai.ngoxdb.core.converter.DatabaseConverter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

/**
 * @author baikai.liao
 * @Time 2021-03-04 23:05:01
 */
@Slf4j
public class PgDatabaseComparator extends BasicDatabaseComparator {

    public PgDatabaseComparator(DatabaseConverter databaseConverter) {
        super(databaseConverter);
    }

    @Override
    public Logger getLogger() {
        return log;
    }
}
