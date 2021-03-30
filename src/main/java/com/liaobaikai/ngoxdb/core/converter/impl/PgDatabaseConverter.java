package com.liaobaikai.ngoxdb.core.converter.impl;

import com.liaobaikai.ngoxdb.bean.ColumnType;
import com.liaobaikai.ngoxdb.bean.NgoxDbMaster;
import com.liaobaikai.ngoxdb.bean.info.IndexInfo2;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.core.comparator.DatabaseComparator;
import com.liaobaikai.ngoxdb.core.comparator.impl.PgDatabaseComparator;
import com.liaobaikai.ngoxdb.core.constant.JdbcDataType;
import com.liaobaikai.ngoxdb.core.converter.BasicDatabaseConverter;
import com.liaobaikai.ngoxdb.core.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.core.dao.impl.PgDatabaseDao;
import com.liaobaikai.ngoxdb.core.dialect.DatabaseDialect;
import com.liaobaikai.ngoxdb.core.dialect.impl.PgDatabaseDialect;
import com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * PostgreSQL数据库转换器
 *
 * @author baikai.liao
 * @Time 2021-02-02 09:00:40
 */
@Slf4j
@Service
public class PgDatabaseConverter extends BasicDatabaseConverter {

    private final PgDatabaseDao databaseDao;
    private final PgDatabaseComparator databaseComparator;
    private UUIDStatus uuidStatus;

    /**
     * UUID状态
     */
    enum UUIDStatus {
        ENABLED,        // 已启用
        NOT_EXISTS,     // 不存在，需执行：CREATE EXTENSION pgcrypto;
        NO_PERMISSION   // 无权限，已执行过：CREATE EXTENSION pgcrypto，但执行失败！
    }

    public PgDatabaseConverter() {
        this.databaseDao = null;
        this.databaseComparator = null;
    }

    public PgDatabaseConverter(NgoxDbMaster ngoxDbMaster) {
        super(ngoxDbMaster);
        this.databaseDao = new PgDatabaseDao(ngoxDbMaster);
        this.databaseComparator = new PgDatabaseComparator(this);
    }

    @Override
    public Logger getLogger() {
        return log;
    }

    @Override
    public BasicDatabaseDao getDatabaseDao() {
        return this.databaseDao;
    }

    @Override
    public Class<? extends DatabaseDialect> getDatabaseDialectClass() {
        return PgDatabaseDialect.class;
    }

    @Override
    public String getDatabaseVendor() {
        return DatabaseVendorEnum.POSTGRESQL.getVendor();
    }

    @Override
    protected String getIdentityColumnString(ColumnType columnType) {

        switch (columnType.getJdbcDataType()) {
            case JdbcDataType.SMALLINT:
                // int2
                return "SMALLSERIAL";
            case JdbcDataType.BIGINT:
                // int8
                return "BIGSERIAL";
        }

        // int4;
        return super.getIdentityColumnString(columnType);
    }

    // @Override
    // public String this.toLookupName(String name) {
    //     // postgres=# \d
    //     //          List of relations
    //     //  Schema |  Name  | Type  |  Owner
    //     // --------+--------+-------+----------
    //     //  public | TABLE2 | table | postgres
    //     //  public | table2 | table | postgres
    //     // 经测试发现，PostgreSQL使用双引号创建表的时候，会保留大小写。
    //     // 不用双引号的时候，默认会转成小写。
    //     //
    //     return "\"" + name.toLowerCase() + "\"";
    // }
    //
    // @Override
    // public String getDatabaseDataTypeDefault(String masterDataTypeDef, int dataType) {
    //     String defaultValue = super.getDatabaseDataTypeDefault(masterDataTypeDef, dataType);
    //     if (dataType == Types.BIT) {
    //         // bit -> boolean
    //         if ("1".equals(masterDataTypeDef)) {
    //             return "true";
    //         } else if ("0".equals(masterDataTypeDef)) {
    //             return "false";
    //         }
    //     }
    //     if(this.getDatabaseFunctionMap().get(StringFunctionEnum.uuid)[0].equals(defaultValue) && this.uuidStatus == null){
    //         // gen_random_uuid()
    //         // 检查是否启用了gen_random_uuid()。
    //         try{
    //             Map<String, Object> uuidRow = this.getDatabaseDao().queryForMap("select " + defaultValue);
    //             if(uuidRow.size() > 0){
    //                 this.uuidStatus = UUIDStatus.ENABLED;
    //             }
    //         }catch (Exception e){
    //             // ERROR: function gen_random_uuid() does not exist
    //             this.getLogger().error(e.getMessage());
    //             this.getLogger().info("[{}] CREATE EXTENSION pgcrypto", this.getDatabaseConfig().getName());
    //             this.uuidStatus = UUIDStatus.NOT_EXISTS;
    //             try {
    //                 this.getDatabaseDao().execute("CREATE EXTENSION pgcrypto");
    //
    //                 Map<String, Object> uuidRow = this.getDatabaseDao().queryForMap("select " + defaultValue);
    //                 if(uuidRow.size() > 0){
    //                     this.uuidStatus = UUIDStatus.ENABLED;
    //                 }
    //
    //             } catch (Exception e2){
    //                 this.getLogger().error(e2.getMessage());
    //                 this.uuidStatus = UUIDStatus.NO_PERMISSION;
    //             }
    //         }
    //
    //         if(!this.uuidStatus.equals(UUIDStatus.ENABLED)){
    //             this.getLogger().warn("[{}] ERROR:  function gen_random_uuid() does not exist！expect:[ CREATE EXTENSION pgcrypto ]", this.getDatabaseConfig().getName());
    //         }
    //
    //     }
    //     return defaultValue;
    // }


    @Override
    protected boolean buildIndexExtensionClause(StringBuilder sBuilder, IndexInfo2 ii, TableInfo ti) {

        if (ii.getType() == IndexInfo2.tableIndexFullText) {
            sBuilder.append(" USING GIN(to_tsvector('english', ");

            // 多个列
            // 获取所有的columnName
            List<String> indexColumns = new ArrayList<>();
            for (IndexInfo2 ii2 : ti.getIndexInfo()) {
                if (!ii.getIndexName().equals(ii2.getIndexName())) {
                    continue;
                }
                indexColumns.add(this.toLookupName(ii2.getColumnName()));
            }

            for (int i = 0, len = indexColumns.size(); i < len; i++) {
                sBuilder.append(indexColumns.get(i));
                if (i != len - 1) {
                    sBuilder.append(" || ' ' || ");
                }
            }

            sBuilder.append("))");

            return true;
        }

        return super.buildIndexExtensionClause(sBuilder, ii, ti);
    }

    @Override
    public DatabaseComparator getComparator() {
        return this.databaseComparator;
    }


}
