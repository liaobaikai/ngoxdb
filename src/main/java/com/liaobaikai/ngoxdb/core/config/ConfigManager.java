package com.liaobaikai.ngoxdb.core.config;

import com.liaobaikai.ngoxdb.boot.JdbcTemplate2;
import com.liaobaikai.ngoxdb.core.enums.fn.DatabaseFunctionEnum;
import com.liaobaikai.ngoxdb.core.service.DatabaseConverter;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.Environment;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * 配置管理器
 *
 * @author baikai.liao
 * @Time 2021-01-25 15:36:39
 */
@Slf4j
@Component
public class ConfigManager implements BeanFactoryAware {

    private static final String MASTER_DATASOURCE_CONFIG_PREFIX = "ngoxdb.master";
    private static final String SLAVE_DATASOURCE_CONFIG_PREFIX = "ngoxdb.slave";

    // private static final Map<String, DatabaseConverter> DATABASE_CONVERTER_MAP = new HashMap<>();

    private static final List<DatabaseConverter> MASTER_DATABASE_CONVERTER = new ArrayList<>(1);
    private static final List<DatabaseConverter> SLAVE_DATABASE_CONVERTERS = new ArrayList<>();

    // private static String masterDatabaseConverterBeanName;
    // private static final List<String> SLAVE_DATABASE_CONVERTER_BEAN_NAMES = new ArrayList<>();

    private final List<JdbcTemplate2> jdbcTemplates = new ArrayList<>();
    // private final Map<String, String> registerBeanNames = new HashMap<>();

    private JdbcTemplate2 jdbcTemplate;

    /**
     * 数据库厂家函数注册中心
     */
    private static final Map<String, Map<DatabaseFunctionEnum, String[]>> DATABASE_VENDOR_DATE_TYPE_CONTAINER = new HashMap<>();

    /////////////////////// 配置信息 ///////////////////////

    @Autowired
    public void setEnvironment(Environment environment) {
        Binder binder = Binder.get(environment);
        // master
        DatabaseConfig databaseConfig = binder.bind(MASTER_DATASOURCE_CONFIG_PREFIX, Bindable.of(DatabaseConfig.class)).get();
        databaseConfig.setName(" MASTER");
        log.info("Master, Database vendor: {}, Username: {}, JDBC-URL: {}", databaseConfig.getDatabase(), databaseConfig.getUsername(), databaseConfig.getUrl());
        DataSource ds = buildDataSource(databaseConfig, "master-data-source");
        jdbcTemplate = new JdbcTemplate2(ds, databaseConfig);
        jdbcTemplates.add(jdbcTemplate);


        // slave
        List<DatabaseConfig> databaseConfigs;
        try {
            databaseConfigs = binder.bind(SLAVE_DATASOURCE_CONFIG_PREFIX, Bindable.listOf(DatabaseConfig.class)).get();
        } catch (NoSuchElementException e) {
            databaseConfigs = new ArrayList<>();
            databaseConfig = binder.bind(SLAVE_DATASOURCE_CONFIG_PREFIX, Bindable.of(DatabaseConfig.class)).get();
            databaseConfigs.add(databaseConfig);
        }

        for (int i = 0; i < databaseConfigs.size(); i++) {
            databaseConfig = databaseConfigs.get(i);
            ds = buildDataSource(databaseConfig, "slave-data-source" + i);
            log.info("SLAVE-{}, Database vendor: {}, Username: {}, JDBC-URL: {}", (i + 1), databaseConfig.getDatabase(), databaseConfig.getUsername(), databaseConfig.getUrl());
            databaseConfig.setName("SLAVE-" + (i + 1));
            jdbcTemplates.add(new JdbcTemplate2(ds, databaseConfig));
        }
    }

    /**
     * 创建数据源
     *
     * @param dataBaseConfig 数据库配置信息
     * @param dataSourceName 数据源名称
     * @return
     */
    private DataSource buildDataSource(DatabaseConfig dataBaseConfig, String dataSourceName) {
        if (!dataBaseConfig.isValid()) {
            return null;
        }
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(dataBaseConfig.getUrl());
        dataSource.setDriverClassName(dataBaseConfig.getDriverClassName());
        dataSource.setUsername(dataBaseConfig.getUsername());
        dataSource.setPassword(dataBaseConfig.getPassword());
        return dataSource;
    }


    ///////////////////////////////// init end /////////////////////////////////


    @Override
    public void setBeanFactory(@NonNull BeanFactory beanFactory) throws BeansException {

        DefaultListableBeanFactory listableBeanFactory = (DefaultListableBeanFactory) beanFactory;
        // 所有的实现类名称
        String[] beanNamesForType = listableBeanFactory.getBeanNamesForType(DatabaseConverter.class);

        // 重新注册
        jdbcTemplates.forEach(jdbcTemplate2 -> {

            for (String beanName : beanNamesForType) {

                // 主库需要注入这个类。
                BeanDefinition beanDefinition = listableBeanFactory.getBeanDefinition(beanName);
                if (beanDefinition.getBeanClassName() == null) {
                    continue;
                }

                DatabaseConverter databaseConverter = (DatabaseConverter) listableBeanFactory.getBean(beanName);
                if (jdbcTemplate2.getDatabaseConfig().getDatabase().equals(databaseConverter.getDatabaseVendor())) {

                    // 注册时间默认值到databaseVendorDateTypeMap
                    if (!DATABASE_VENDOR_DATE_TYPE_CONTAINER.containsKey(databaseConverter.getDatabaseVendor())) {
                        DATABASE_VENDOR_DATE_TYPE_CONTAINER.put(databaseConverter.getDatabaseVendor(), databaseConverter.getDatabaseFunctionMap());
                    }

                    // 默认的是否存在
                    // String newBeanName;
                    boolean isMaster = jdbcTemplate2 == jdbcTemplate;

                    // if(!registerBeanNames.containsKey(beanName)){
                    //     newBeanName = beanName;
                    // } else {
                    //     for(int i = 0; ; i++){
                    //         newBeanName = String.format("%s-%s-%d", beanName, (isMaster ? "master": "slave"), i);
                    //         if(!registerBeanNames.containsKey(newBeanName)){
                    //             break;
                    //         }
                    //     }
                    // }

                    try {
                        Class<?> clazz = Class.forName(beanDefinition.getBeanClassName());
                        Constructor<?> constructor = clazz.getConstructor(JdbcTemplate2.class, boolean.class, String.class, DatabaseConfig.class);
                        DatabaseConverter converter = (DatabaseConverter) constructor.newInstance(
                                jdbcTemplate2,
                                isMaster,
                                jdbcTemplate.getDatabaseConfig().getDatabase(),
                                jdbcTemplate2.getDatabaseConfig()
                        );

                        if (isMaster) {
                            MASTER_DATABASE_CONVERTER.add(converter);
                        } else {
                            SLAVE_DATABASE_CONVERTERS.add(converter);
                        }

                    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                        e.printStackTrace();
                    }


                    // if(isMaster){
                    //     masterDatabaseConverterBeanName = newBeanName;
                    // } else {
                    //     SLAVE_DATABASE_CONVERTER_BEAN_NAMES.add(newBeanName);
                    // }
                    //
                    //
                    // BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.genericBeanDefinition(beanDefinition.getBeanClassName());
                    // beanDefinitionBuilder.addConstructorArgValue(jdbcTemplate2)
                    //                      .addConstructorArgValue(isMaster)
                    //                      .addConstructorArgValue(jdbcTemplate.getDatabaseConfig().getDatabase());
                    // BeanUtils.copyProperties(beanDefinition, beanDefinitionBuilder);
                    // listableBeanFactory.registerBeanDefinition(newBeanName, beanDefinitionBuilder.getBeanDefinition());
                    // registerBeanNames.put(newBeanName, beanDefinition.getBeanClassName());

                    break;
                }
            }

        });


        // 如果存在这个bean
        // pendingDestroyBeans.forEach(listableBeanFactory::destroyBean);

        // DATABASE_CONVERTER_MAP.putAll(listableBeanFactory.getBeansOfType(DatabaseConverter.class));
        //
        // MASTER_DATABASE_CONVERTER.add(DATABASE_CONVERTER_MAP.get(masterDatabaseConverterBeanName));
        // SLAVE_DATABASE_CONVERTER_BEAN_NAMES.forEach(beanName -> SLAVE_DATABASE_CONVERTERS.add(DATABASE_CONVERTER_MAP.get(beanName)));

    }

    public static DatabaseConverter getMasterDatabaseConverter() {
        if (MASTER_DATABASE_CONVERTER.size() == 0) {
            throw new NullPointerException("masterDatabaseConverter count is 0");
        }
        return MASTER_DATABASE_CONVERTER.get(0);
    }

    public static List<DatabaseConverter> getSlaveDatabaseConverters() {
        return SLAVE_DATABASE_CONVERTERS;
    }

    /**
     * 获取目标数据库的默认值，从源数据库类型/函数转换为目标类型/函数
     *
     * @param masterDateTypeDefault 主数据库的时间默认值
     * @param masterDatabaseVendor  主数据库厂家
     * @param slaveDatabaseVendor   从数据库厂家
     * @return 时间默认值
     */
    public static String getDatabaseDataTypeDefault(String masterDateTypeDefault,
                                                    String masterDatabaseVendor,
                                                    String slaveDatabaseVendor) {

        if (masterDateTypeDefault == null || masterDateTypeDefault.length() == 0) {
            return null;
        }

        Map<DatabaseFunctionEnum, String[]> databaseFunctionEnumMap = DATABASE_VENDOR_DATE_TYPE_CONTAINER.get(masterDatabaseVendor);

        // 获取时间默认值对用的主数据库中的DateTypeEnum。
        boolean isJump = false;

        // 相等匹配
        DatabaseFunctionEnum masterDatabaseFunctionEnum = null;
        for (Map.Entry<DatabaseFunctionEnum, String[]> en : databaseFunctionEnumMap.entrySet()) {
            if (isJump) {
                break;
            }
            for (String value : en.getValue()) {
                if (StringUtils.isNotEmpty(value) && masterDateTypeDefault.equalsIgnoreCase(value)) {
                    isJump = true;
                    masterDatabaseFunctionEnum = en.getKey();
                    break;
                }
            }
        }

        // 模糊匹配
        if(masterDatabaseFunctionEnum == null){
            for (Map.Entry<DatabaseFunctionEnum, String[]> en : databaseFunctionEnumMap.entrySet()) {
                if (isJump) {
                    break;
                }
                for (String value : en.getValue()) {
                    // 首先全部匹配。如果匹配不到，则使用包含的方式。
                    if (StringUtils.isNotEmpty(value) && masterDateTypeDefault.toLowerCase().contains(value.toLowerCase())) {
                        isJump = true;
                        masterDatabaseFunctionEnum = en.getKey();
                        break;
                    }
                }
            }
        }


        // 匹配不到，不支持的时间默认值
        if (masterDatabaseFunctionEnum == null) {
            return null;
        }

        Map<DatabaseFunctionEnum, String[]> slaveDateTypeEnumMap = DATABASE_VENDOR_DATE_TYPE_CONTAINER.get(slaveDatabaseVendor);

        if (slaveDateTypeEnumMap == null) {
            // 不支持
            return null;
        }

        // 获取对应的时间默认值
        String[] values = slaveDateTypeEnumMap.get(masterDatabaseFunctionEnum);
        // 默认获取第一个
        if (values == null || values.length == 0) {
            return null;
        }
        // 匹配不到
        return values[0];
    }

}
