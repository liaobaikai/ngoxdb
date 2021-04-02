package com.liaobaikai.ngoxdb.anno;

import com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum;

import java.lang.annotation.ElementType;

/**
 * @author baikai.liao
 * @Time 2021-03-29 13:16:30
 */
@java.lang.annotation.Target({ElementType.FIELD})
@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
@java.lang.annotation.Documented
public @interface Description {

    String name();

    String label();

    String defaultValue() default "";

    DatabaseVendorEnum[] applyOn() default {};

}
