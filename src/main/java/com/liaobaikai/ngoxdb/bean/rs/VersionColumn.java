package com.liaobaikai.ngoxdb.bean.rs;

import lombok.Getter;
import lombok.Setter;

/**
 * @author baikai.liao
 * @Time 2021-01-27 15:44:36
 */
@Setter
@Getter
public class VersionColumn {

    private short scope;
    private String columnName;
    private int dataType;
    private String typeName;
    private int columnSize;
    private int bufferLength;
    private int decimalDigits;
    private short pseudoColumn;
}
