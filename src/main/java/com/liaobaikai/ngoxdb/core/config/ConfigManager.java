package com.liaobaikai.ngoxdb.core.config;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSONObject;
import com.liaobaikai.ngoxdb.bean.NgoxDbMaster;
import com.liaobaikai.ngoxdb.boot.JdbcTemplate;
import com.liaobaikai.ngoxdb.core.converter.DatabaseConverter;
import com.liaobaikai.ngoxdb.core.dialect.DatabaseDialect;
import com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum;
import com.liaobaikai.ngoxdb.core.enums.func.DatabaseFunctionEnum;
import com.liaobaikai.ngoxdb.core.func.SQLFunction;
import lombok.extern.slf4j.Slf4j;
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

    private static final String MASTER_DATASOURCE_CONFIG_PREFIX = "master";
    private static final String SLAVE_DATASOURCE_CONFIG_PREFIX = "slave";

    private static final List<DatabaseConverter> MASTER_DATABASE_CONVERTER = new ArrayList<>(1);
    private static final List<DatabaseConverter> SLAVE_DATABASE_CONVERTERS = new ArrayList<>();

    private final Map<DatabaseConfig, JdbcTemplate> mapOfJdbcTemplate = new HashMap<>();

    private DatabaseConfig mDatabaseConfig;
    private Map<Class<? extends DatabaseDialect>, DatabaseDialect> databaseDialectMap = new HashMap<>();

    /**
     * 数据库厂家函数注册中心
     */
    private static final Map<String, Map<DatabaseFunctionEnum, SQLFunction[]>> DATABASE_VENDOR_DATE_TYPE_CONTAINER = new HashMap<>();

    /////////////////////// 配置信息 ///////////////////////

    @Autowired
    public void setEnvironment(Environment environment) {
        Binder binder = Binder.get(environment);
        // master
        mDatabaseConfig = binder.bind(MASTER_DATASOURCE_CONFIG_PREFIX, Bindable.of(DatabaseConfig.class)).get();
        mDatabaseConfig.setName(" MASTER");
        log.info("Master, Database vendor: {}, Username: {}, JDBC-URL: {}", mDatabaseConfig.getDatabase(), mDatabaseConfig.getUsername(), mDatabaseConfig.getUrl());
        DataSource ds = buildDataSource(mDatabaseConfig, "master-data-source");
        mapOfJdbcTemplate.put(mDatabaseConfig, new JdbcTemplate(ds));

        // 注册多个连接

        // slave
        List<DatabaseConfig> databaseConfigs;
        DatabaseConfig databaseConfig;
        try {
            databaseConfigs = binder.bind(SLAVE_DATASOURCE_CONFIG_PREFIX, Bindable.listOf(DatabaseConfig.class)).get();
        } catch (NoSuchElementException e) {
            databaseConfigs = new ArrayList<>();
            databaseConfig = binder.bind(SLAVE_DATASOURCE_CONFIG_PREFIX, Bindable.of(DatabaseConfig.class)).get();
            databaseConfigs.add(databaseConfig);
        }

        for (int i = 0; i < databaseConfigs.size(); i++) {
            databaseConfig = databaseConfigs.get(i);
            databaseConfig.setName(String.format("SLAVE-%s", i));

            log.info("SLAVE-{}, Database vendor: {}, Username: {}, JDBC-URL: {}", i, databaseConfig.getDatabase(), databaseConfig.getUsername(), databaseConfig.getUrl());
            ds = buildDataSource(databaseConfig, String.format("slave-data-source-%s", i));
            mapOfJdbcTemplate.put(databaseConfig, new JdbcTemplate(ds));
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
            throw new IllegalArgumentException("datasource is invalid: " + JSONObject.toJSONString(dataBaseConfig));
        }
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setName(dataSourceName);
        dataSource.setUrl(dataBaseConfig.getUrl());
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
        for (Map.Entry<DatabaseConfig, JdbcTemplate> en : this.mapOfJdbcTemplate.entrySet()) {
            JdbcTemplate jdbcTemplate = en.getValue();
            DatabaseConfig databaseConfig = en.getKey();

            for (String beanName : beanNamesForType) {

                // 主库需要注入这个类。
                BeanDefinition beanDefinition = listableBeanFactory.getBeanDefinition(beanName);
                if (beanDefinition.getBeanClassName() == null) {
                    continue;
                }

                DatabaseConverter registeredConverter = (DatabaseConverter) listableBeanFactory.getBean(beanName);
                if (registeredConverter.getDatabaseVendor() == null) {
                    throw new NullPointerException(registeredConverter.getClass().getName() + ".getDatabaseVendor() not implement!");
                }

                if (databaseConfig.getDatabase().equals(registeredConverter.getDatabaseVendor())) {

                    boolean isMaster = databaseConfig == mDatabaseConfig;

                    try {
                        // 获取对应的方言Class
                        DatabaseDialect databaseDialect = databaseDialectMap.get(registeredConverter.getDatabaseDialectClass());
                        if (databaseDialect == null) {
                            databaseDialect = registeredConverter.getDatabaseDialectClass().newInstance();
                            databaseDialectMap.put(registeredConverter.getDatabaseDialectClass(), databaseDialect);
                        }

                        Class<?> clazz = Class.forName(beanDefinition.getBeanClassName());
                        Constructor<?> constructor = clazz.getConstructor(NgoxDbMaster.class);

                        NgoxDbMaster ngoxDbMaster = new NgoxDbMaster();
                        ngoxDbMaster.setJdbcTemplate(jdbcTemplate);
                        ngoxDbMaster.setDatabaseDialect(databaseDialect);
                        ngoxDbMaster.setMaster(isMaster);
                        ngoxDbMaster.setMasterDatabaseVendor(mDatabaseConfig.getDatabase());
                        ngoxDbMaster.setDatabaseConfig(databaseConfig);

                        DatabaseConverter converter = (DatabaseConverter) constructor.newInstance(ngoxDbMaster);

                        // 注册函数
                        if (!DATABASE_VENDOR_DATE_TYPE_CONTAINER.containsKey(converter.getDatabaseVendor())) {
                            DATABASE_VENDOR_DATE_TYPE_CONTAINER.put(converter.getDatabaseVendor(), databaseDialect.getSQLFunctionMap());
                        }

                        if (isMaster) {
                            MASTER_DATABASE_CONVERTER.add(converter);
                        } else {
                            SLAVE_DATABASE_CONVERTERS.add(converter);
                        }

                    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                        e.printStackTrace();
                    }

                    break;
                }
            }
        }
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

        // 相同的厂家，直接返回。
        if (DatabaseVendorEnum.isSameDatabaseVendor(masterDatabaseVendor, slaveDatabaseVendor)) {
            return masterDateTypeDefault;
        }

        if (masterDateTypeDefault == null || masterDateTypeDefault.length() == 0) {
            return null;
        }

        Map<DatabaseFunctionEnum, SQLFunction[]> databaseFunctionEnumMap = DATABASE_VENDOR_DATE_TYPE_CONTAINER.get(masterDatabaseVendor);

        // 获取时间默认值对用的主数据库中的DateTypeEnum。
        boolean isJump = false;

        // 相等匹配
        DatabaseFunctionEnum masterDatabaseFunctionEnum = null;
        for (Map.Entry<DatabaseFunctionEnum, SQLFunction[]> en : databaseFunctionEnumMap.entrySet()) {
            if (isJump) {
                break;
            }
            for (SQLFunction sqlFunction : en.getValue()) {
                if (masterDateTypeDefault.equalsIgnoreCase(sqlFunction.build())) {
                    isJump = true;
                    masterDatabaseFunctionEnum = en.getKey();
                    break;
                }
            }
        }

        // 模糊匹配
        if (masterDatabaseFunctionEnum == null) {
            for (Map.Entry<DatabaseFunctionEnum, SQLFunction[]> en : databaseFunctionEnumMap.entrySet()) {
                if (isJump) {
                    break;
                }
                for (SQLFunction sqlFunction : en.getValue()) {
                    // 首先全部匹配。如果匹配不到，则使用包含的方式。
                    if (masterDateTypeDefault.toLowerCase().contains(sqlFunction.build())) {
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

        Map<DatabaseFunctionEnum, SQLFunction[]> slaveDateTypeEnumMap = DATABASE_VENDOR_DATE_TYPE_CONTAINER.get(slaveDatabaseVendor);

        if (slaveDateTypeEnumMap == null) {
            // 不支持
            return null;
        }

        // 获取对应的时间默认值
        SQLFunction[] sqlFunctions = slaveDateTypeEnumMap.get(masterDatabaseFunctionEnum);
        // 默认获取第一个
        if (sqlFunctions == null || sqlFunctions.length == 0) {
            return null;
        }
        // 匹配不到
        return sqlFunctions[0].build();
    }

}
