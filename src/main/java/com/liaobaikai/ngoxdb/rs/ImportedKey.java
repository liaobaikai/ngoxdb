package com.liaobaikai.ngoxdb.rs;

import lombok.Getter;
import lombok.Setter;

import java.sql.DatabaseMetaData;

/**
 * 本表引用外表的键
 *
 * @author baikai.liao
 * @Time 2021-01-27 15:36:30
 */
@Setter
@Getter
public class ImportedKey {

    // 引用表的信息：REFERENCES ....
    /**
     * 引用表的数据库名
     */
    private String pkTableCat;

    /**
     * 引用表的模式，可能为NULL
     */
    private String pkTableSchem;

    /**
     * 引用表的表名
     */
    private String pkTableName;

    /**
     * 引用表的列名
     */
    private String pkColumnName;

    // 创建外键表的信息
    /**
     * 外键表的数据库名
     */
    private String fkTableCat;

    /**
     * 外键表的默认，可能为NULL
     */
    private String fkTableSchem;

    /**
     * 外键表的表名
     */
    private String fkTableName;

    /**
     * 外键表的列名
     */
    private String fkColumnName;

    /**
     * 序号
     */
    private short keySeq;

    /**
     * 更新的规则
     * {@link java.sql.DatabaseMetaData}
     * 0: CASCADE
     * 1: RESTRICT
     * 2: SET_NULL
     * 3: NO_ACTION
     * 4: SET_DEFAULT
     */
    public static final short CASCADE = DatabaseMetaData.importedKeyCascade;
    public static final short RESTRICT = DatabaseMetaData.importedKeyRestrict;
    public static final short SET_NULL = DatabaseMetaData.importedKeySetNull;
    public static final short NO_ACTION = DatabaseMetaData.importedKeyNoAction;
    public static final short SET_DEFAULT = DatabaseMetaData.importedKeySetDefault;

    private short updateRule = CASCADE;

    /**
     * 删除的规则
     * {@link java.sql.DatabaseMetaData}
     * 0: CASCADE
     * 1: RESTRICT
     * 2: SET_NULL
     * 3: NO_ACTION
     * 4: SET_DEFAULT
     */
    private short deleteRule;

    /**
     * 外键表的外键名称
     */
    private String fkName;

    /**
     * 引用表的主键名称
     */
    private String pkName;

    /**
     * 外键约束的评估是否可以推迟到提交
     */
    private short deferrability;

    /**
     * 获取动作的名称
     * @param action 规则
     * @return String
     */
    private String getActionName(short action){

        switch (action){
            case RESTRICT:
                return "RESTRICT";
            case SET_NULL:
                return "SET NULL";
            case NO_ACTION:
                return "NO ACTION";
            case SET_DEFAULT:
                return "SET DEFAULT";
            default:
                return "CASCADE";
        }
    }

    /**
     * 删除的动作
     * @return ON DELETE 'ActionName'
     */
    public String getDeleteAction(){
        return "ON DELETE " + this.getActionName(this.deleteRule);
    }

    /**
     * 更新的动作
     * @return ON UPDATE 'ActionName'
     */
    public String getUpdateAction(){
        return "ON UPDATE " + this.getActionName(this.updateRule);
    }

}
