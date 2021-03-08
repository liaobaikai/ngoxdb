package com.liaobaikai.ngoxdb.bean.info;

import com.liaobaikai.ngoxdb.bean.rs.TableBase;
import lombok.Getter;
import lombok.Setter;

/**
 * 检查约束
 *
 * @author baikai.liao
 * @Time 2021-01-28 12:05:30
 */
@Setter
@Getter
public class ConstraintInfo extends TableBase {

    /**
     * 检查约束
     */
    public static final String TYPE_CHECK_CONSTRAINT = "C";

    /**
     * 约束名称
     */
    private String constraintName;

    /**
     * 约束类型
     */
    private String constraintType;

    /**
     * 检查条件
     */
    private String checkCondition;

    /**
     * 原来的检查条件，未经修改的。
     */
    private String sourceCheckCondition;


}
