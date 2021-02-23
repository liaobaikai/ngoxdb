package com.liaobaikai.ngoxdb.rs;

import lombok.Getter;
import lombok.Setter;

/**
 * 表信息
 * DatabaseMetaData.getTables()
 *
 * @author baikai.liao
 * @Time 2021-01-27 15:02:34
 */
@Setter
@Getter
public class Table extends TableBase {

    /**
     * 表的类型
     */
    // "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM"
    private String tableType;

    /**
     * 表注释
     */
    private String remarks;

    /**
     * the types catalog (may be null)
     */
    private String typeCat;

    /**
     * the types schema (may be null)
     */
    private String typeSchem;

    /**
     * type name (may be null)
     */
    private String typeName;

    /**
     * name of the designated "identifier" column of a typed table (may be null)
     */
    private String selfReferencingColName;

    /**
     * Values are "SYSTEM", "USER", "DERIVED". (may be null)
     */
    private String refGeneration;


}
