package com.liaobaikai.ngoxdb;

import com.liaobaikai.ngoxdb.info.TableInfo;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class BootApplicationTests {

    // @Autowired
    // private MySQLConverter destConverter;
    //
    // @Autowired
    // private MySQLConverter sourceConverter;

    @Test
    void contextLoads() {
        // TableInfo tableInfo = sourceConverter.getTableInfo("actor2");

        // 源转换器生成表信息
        // System.out.println(JSONObject.toJSONString(tableInfo,
        //         SerializerFeature.WRITE_MAP_NULL_FEATURES,
        //         SerializerFeature.PrettyFormat,
        //         SerializerFeature.DisableCircularReferenceDetect));
        // 目标转换器生成表
        // tableInfo.setTableName("test102");
        // destConverter.createTable(tableInfo);
        // 创建元数据（默认值，索引、检查约束、外键）
        // destConverter.applySlaveMetaData();
        // 从源转换到目标
        // sourceConverter.transform(destConverter, "dbo", "Persons");

    }

}
