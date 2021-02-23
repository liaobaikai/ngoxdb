package com.liaobaikai.ngoxdb;

import com.liaobaikai.ngoxdb.config.ConfigManager;
import com.liaobaikai.ngoxdb.info.TableInfo;
import com.liaobaikai.ngoxdb.service.DatabaseConverter;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.List;


// https://docs.oracle.com/cloud/help/zh_CN/analytics-cloud/ACSDS/GUID-33F45B17-782F-4A56-9FA9-7163A3BD79B1.htm#ACSDS-GUID-5080E628-864A-4024-B06A-764AED575909
//
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class BootApplication {

    public static void main(String[] args) {
        SpringApplication springApplication = new SpringApplication(BootApplication.class);
        springApplication.setBannerMode(Banner.Mode.OFF);
        springApplication.run(args);

        DatabaseConverter masterConverter = ConfigManager.getMasterDatabaseConverter();
        List<TableInfo> tis = masterConverter.getTableInfo();
        // ConfigManager.getSlaveDatabaseConverters().forEach(converter -> new Thread(() -> {
        //     converter.createAllTable(tis);
        //     converter.applySlaveDatabaseMetadata();
        // }).start());
        ConfigManager.getSlaveDatabaseConverters().forEach(converter -> {
            converter.createAllTable(tis);
            converter.applySlaveDatabaseMetadata();
        });
    }

}
