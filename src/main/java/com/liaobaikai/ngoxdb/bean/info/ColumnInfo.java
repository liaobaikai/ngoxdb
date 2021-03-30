package com.liaobaikai.ngoxdb.bean.info;

import com.liaobaikai.ngoxdb.bean.ColumnType;
import com.liaobaikai.ngoxdb.bean.rs.Column;
import lombok.Getter;
import lombok.Setter;

import java.sql.DatabaseMetaData;
import java.util.HashMap;
import java.util.Map;

/**
 * @author baikai.liao
 * @Time 2021-01-27 18:41:54
 */
@Setter
@Getter
public class ColumnInfo extends Column {

    public boolean isNotNull() {
        return this.getNullable() == DatabaseMetaData.attributeNoNulls;
    }

    /**
     * 字段类型，如varchar(10), set('Y', 'N'), enum('Y', 'N'), float(10, 3), 如果数据库不存在，需要自己拼接。
     */
    private String columnType;

    /**
     * 额外的信息：如mysql的 show columns.. (Extra)
     */
    private String extra;

    /**
     * 字符集
     */
    private String charsetName;

    /**
     * 排序规则
     */
    private String collationName;

    /**
     * 是否为自动增长
     *
     * @return true, false
     */
    public boolean isAutoincrement() {
        return YES.equalsIgnoreCase(getIsAutoincrement());
    }

    /**
     * 是否为自动增长
     *
     * @return true, false
     */
    public boolean isGeneratedColumn() {
        return YES.equalsIgnoreCase(getIsGeneratedColumn());
    }

    /**
     * 是否为无符号
     */
    private boolean isUnsigned;

    /**
     * 数据库厂家转换后的类型映射
     * 数据库厂家 <-> 转换后的类型信息
     */
    private Map<String, ColumnType> mapOfColumnType = new HashMap<>();
}
