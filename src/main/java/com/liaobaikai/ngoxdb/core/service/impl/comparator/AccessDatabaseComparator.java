package com.liaobaikai.ngoxdb.core.service.impl.comparator;

import com.liaobaikai.ngoxdb.core.service.DatabaseConverter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

/**
 * @author baikai.liao
 * @Time 2021-03-04 23:03:50
 */
@Slf4j
public class AccessDatabaseComparator extends BasicDatabaseComparator {

    public AccessDatabaseComparator(DatabaseConverter databaseConverter) {
        super(databaseConverter);
    }

    @Override
    public Logger getLogger() {
        return log;
    }
}
