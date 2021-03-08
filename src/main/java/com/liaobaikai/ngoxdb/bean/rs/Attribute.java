package com.liaobaikai.ngoxdb.bean.rs;

import lombok.Getter;
import lombok.Setter;

/**
 * user-defined type (UDT)
 *
 * @author baikai.liao
 * @Time 2021-01-27 15:48:18
 */
@Setter
@Getter
public class Attribute {

    private String typeCat;
    private String typeSchem;
    private String typeName;
    private String attrName;
    private int dataType;
    private String attrTypeName;
    private int attrSize;
    private int decimalDigits;
    private int numPrecRadix;
    private int nullable;
    private String remarks;
    private String attrDef;
    private int sqlDataType;
    private int sqlDatetimeSub;
    private int charOctetLength;
    private int ordinalPosition;
    private String isNullable;
    private String scopeCatalog;
    private String scopeSchema;
    private String scopeTable;
    private short sourceDataType;
}
