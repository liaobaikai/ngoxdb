package com.liaobaikai.ngoxdb.core.converter.impl;

import com.liaobaikai.ngoxdb.bean.NgoxDbMaster;
import com.liaobaikai.ngoxdb.bean.info.IndexInfo2;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.bean.rs.ImportedKey;
import com.liaobaikai.ngoxdb.core.comparator.DatabaseComparator;
import com.liaobaikai.ngoxdb.core.comparator.impl.SQLServerDatabaseComparator;
import com.liaobaikai.ngoxdb.core.converter.BasicDatabaseConverter;
import com.liaobaikai.ngoxdb.core.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.core.dao.impl.SQLServerDatabaseDao;
import com.liaobaikai.ngoxdb.core.dialect.DatabaseDialect;
import com.liaobaikai.ngoxdb.core.dialect.impl.SQLServerDatabaseDialect;
import com.liaobaikai.ngoxdb.core.entity.NgoxDbRelayLog;
import com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum;
import com.liaobaikai.ngoxdb.utils.NumberUtils;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Microsoft SQLServer 转换器
 *
 * @author baikai.liao
 * @Time 2021-01-28 15:56:41
 */
@Slf4j
@Service
public class SQLServerDatabaseConverter extends BasicDatabaseConverter {

    private final Map<String, Boolean> tableForeignKeyRuleMap = new HashMap<>();

    /**
     * 只适用于SQLServer，将timestamp类型转换为binary，不然无法导入数据，会抛出如下错误：
     * 不能将显式值插入时间戳列。请对列列表使用 INSERT 来排除时间戳列，或将 DEFAULT 插入时间戳列。
     */
    private boolean timestamp2binary = true;

    private final SQLServerDatabaseDao databaseDao;
    private final SQLServerDatabaseComparator databaseComparator;

    public SQLServerDatabaseConverter() {
        this.databaseDao = null;
        this.databaseComparator = null;
    }

    public SQLServerDatabaseConverter(NgoxDbMaster ngoxDbMaster) {
        super(ngoxDbMaster);
        this.databaseDao = new SQLServerDatabaseDao(ngoxDbMaster);
        this.databaseComparator = new SQLServerDatabaseComparator(this);
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
        return SQLServerDatabaseDialect.class;
    }

    @Override
    protected String buildIndexName(IndexInfo2 ii, TableInfo ti) {

        if (ii.getType() == IndexInfo2.tableIndexFullText && ti.getPrimaryKeys().size() == 1) {
            // 全文索引无需索引名，否则会执行失败！
            return "";
        }

        return super.buildIndexName(ii, ti);
    }

    @Override
    protected void afterBuildIndex(StringBuilder sBuilder, IndexInfo2 ii, TableInfo ti) {

        if (ii.getType() == IndexInfo2.tableIndexFullText && ti.getPrimaryKeys().size() == 1) {
            // 全文索引
            // https://docs.microsoft.com/zh-cn/sql/t-sql/statements/create-fulltext-index-transact-sql?view=sql-server-ver15
            // https://zhuanlan.zhihu.com/p/159211413

            String finalTableName = ti.getTableName();

            // 查询主键的名称
            List<Map<String, Object>> pkList =
                    this.getDatabaseDao().getJdbcTemplate().queryForList(
                            "SELECT NAME FROM SYS.OBJECTS WHERE PARENT_OBJECT_ID = OBJECT_ID(?) AND TYPE = 'PK'", finalTableName);

            if (pkList.size() == 1) {
                String fullTextCatalogName = finalTableName + "_catalog";

                Map<String, Object> result =
                        this.getDatabaseDao().getJdbcTemplate().queryForMap(
                                "SELECT COUNT(NAME) AS COUNT FROM SYS.FULLTEXT_CATALOGS WHERE NAME = ?", fullTextCatalogName);
                int exists = NumberUtils.toInt(result.get("COUNT"));
                if (exists == 0) {
                    this.getDatabaseDao().insertLogRows(new NgoxDbRelayLog(finalTableName, NgoxDbRelayLog.TYPE_OTHER,
                            "create fulltext catalog " + fullTextCatalogName));
                }

                sBuilder.append(" key index ").append(pkList.get(0).get("NAME"))
                        .append(" on ").append(fullTextCatalogName)
                        .append(" with change_tracking auto");
            }
        }

    }

    @Override
    protected void buildForeignKeyChangeRules(StringBuilder sBuilder, ImportedKey importedKey) {
        // 将 FOREIGN KEY 约束 'a' 引入表 'b' 可能会导致循环或多重级联路径。请指定 ON DELETE NO ACTION 或 ON UPDATE NO ACTION，或修改其他 FOREIGN KEY 约束。
        // https://docs.microsoft.com/zh-cn/sql/relational-databases/errors-events/mssqlserver-1785-database-engine-error?view=sql-server-ver15
        // sqlserver不支持存在多个cascade的外键。
        Boolean isDeletedCascade = this.tableForeignKeyRuleMap.get(importedKey.getPkTableName() + "-deleted");
        Boolean isUpdatedCascade = this.tableForeignKeyRuleMap.get(importedKey.getPkTableName() + "-updated");
        boolean f1 = false, f2 = false;

        // 已经存在了cascade
        if (isDeletedCascade != null && isDeletedCascade) {
            // 关键字 'RESTRICT' 附近有语法错误。
            // https://docs.microsoft.com/en-us/sql/relational-databases/tables/primary-and-foreign-key-constraints?view=sql-server-ver15
            // Cascading Referential Integrity
            if (importedKey.getDeleteRule() == ImportedKey.CASCADE) {
                this.getLogger().warn("{}.{} ON DELETE CASCADE -> ON DELETE NO ACTION", importedKey.getPkTableName(), importedKey.getPkColumnName());
                sBuilder.append(" ON DELETE ").append(importedKey.getActionName(ImportedKey.NO_ACTION)).append(" ");
                f1 = true;
            }
        }

        if (importedKey.getDeleteRule() != ImportedKey.RESTRICT && !f1) {
            sBuilder.append(" ").append(importedKey.getDeleteAction()).append(" ");
        }

        // 更新规则
        if (isUpdatedCascade != null && isUpdatedCascade) {
            if (importedKey.getUpdateRule() == ImportedKey.CASCADE) {
                this.getLogger().warn("{}.{} ON UPDATE CASCADE -> ON UPDATE NO ACTION", importedKey.getPkTableName(), importedKey.getPkColumnName());
                sBuilder.append(" ON UPDATE ").append(importedKey.getActionName(ImportedKey.NO_ACTION)).append(" ");
                f2 = true;
            }
        }

        if (importedKey.getUpdateRule() != ImportedKey.RESTRICT && !f2) {
            sBuilder.append(" ").append(importedKey.getUpdateAction()).append(" ");
        }

        // 一个主键表，存在多个外键的cascade引用的话，需要将其他改成NO ACTION
        this.tableForeignKeyRuleMap.put(importedKey.getPkTableName() + "-deleted", importedKey.getDeleteRule() == ImportedKey.CASCADE);
        this.tableForeignKeyRuleMap.put(importedKey.getPkTableName() + "-updated", importedKey.getUpdateRule() == ImportedKey.CASCADE);

    }

    @Override
    public String getDatabaseVendor() {
        return DatabaseVendorEnum.SQLSERVER.getVendor();
    }

    // @Override
    // public void changeBatchArgs(List<Object[]> batchArgs, TableInfo ti) {
    //     // https://docs.microsoft.com/zh-cn/previous-versions/sql/sql-server-2005/ms182776(v=sql.90)?redirectedfrom=MSDN
    //
    //     // TIMESTAMP 不支持设置值。
    //     // 否则会抛出如下错误：
    //     // 不能将显式值插入时间戳列。请对列列表使用 INSERT 来排除时间戳列，或将 DEFAULT 插入时间戳列。
    //
    //     // 需要将具体的参数值设置为 null，SQLServer会自动生成。
    //     for(Object[] objects : batchArgs){
    //         // row
    //         for(int x = 0, len = objects.length; x < len; x++){
    //             // cell
    //             ColumnInfo columnInfo = ti.getColumns().get(x);
    //             if(this.isSameDatabaseVendor() && "timestamp".equals(columnInfo.getTypeName())){
    //                 objects[x] = null;
    //             }
    //         }
    //     }
    // }

    @Override
    public DatabaseComparator getComparator() {
        return this.databaseComparator;
    }


    @Override
    public void afterImportRows(Connection con, long beginTime, int importCount, int offset, int limit, TableInfo ti) {
        super.afterImportRows(con, beginTime, importCount, offset, limit, ti);
    }

    // @Override
    // public void importRows(List<Object[]> batchArgs, TableInfo ti, int offset, int limit) {

    // boolean hasAutoIdentity = this.hasAutoIdentity(ti);

    // try {
    // Sqlserver 仅支持一个表设置 IDENTITY_INSERT ON
    // 当有其他表被设置的时候，需要off
    // 否则会抛出类似如下错误：表 'sakila.dbo.film' 的 IDENTITY_INSERT 已经为 ON。无法为表 'inventory' 执行 SET 操作。
    // 启用自动增长。
    // expect: set IDENTITY_INSERT <table> ON;
    // if(hasAutoIdentity){
    //     // 需要设置 IDENTITY_INSERT ON
    //     this.getDatabaseDao().getJdbcTemplate().execute((ConnectionCallback<Boolean>) con -> {
    //
    //         ISQLServerConnection isqlServerConnection = con.unwrap(ISQLServerConnection.class);
    //
    //         // 判断是否设置了 IDENTITY_INSERT ON
    //         String value = mapOfConnectionTableName.get(isqlServerConnection.toString());
    //         if(value == null){
    //             String stmt = getDatabaseDialect().getDisableIdentityString(ti.getTableName());
    //             getLogger().info("[{}] {}", getDatabaseConfig().getName(), stmt);
    //             con.createStatement().execute(stmt);
    //         } else {
    //             // 不为null，判断设置了的表是否和当前的一致
    //             return value.equals(ti.getTableName());
    //         }
    //
    //         mapOfConnectionTableName.put(isqlServerConnection.toString(), ti.getTableName());
    //         System.out.println(mapOfConnectionTableName);
    //
    //         return true;
    //     });
    //
    //
    //     return;
    //
    //     // String identityInsertOnTable = IdentityInsertUtils.getAfterSetIfUndefined(ti.getTableName(), getDatabaseDao().getJdbcTemplate());
    //     // if(!ti.getTableName().equals(identityInsertOnTable)){
    //     //     if(identityInsertOnTable != null) {
    //     //         this.getDatabaseDao().execute(String.format("set IDENTITY_INSERT %s off", this.toLookupName(identityInsertOnTable)));
    //     //     }
    //     //
    //     //     this.disableIdentity(ti);
    //     // }
    //     // super.importRows(batchArgs, ti, offset, limit);
    //     // return;
    // }


    // CachedRowSet cachedRowSet = new CachedRowSetImpl();
    // cachedRowSet.populate(null);
    // SQLServerBulkCopy sqlServerBulkCopy = new SQLServerBulkCopy(null);
    // ResultSet rs = null;
    // sqlServerBulkCopy.writeToServer(rs);


    // super.importRows(batchArgs, ti, offset, limit);

    // } catch (Exception e){
    //     e.printStackTrace();
    // } finally {
    // if(hasAutoIdentity){
    //     this.identityInsertReentrantLock.unlock();
    // }
    // }
    // }

    // @Override
    // public void applyLog() {
    //     List<NgoxDbRelayLog> list = this.getDatabaseDao().getLogRows(NgoxDbRelayLog.NOT_USED);
    //     for(NgoxDbRelayLog log: list){
    //         apply(log);
    //     }
    // }

    // @Override
    // public void enableIdentity(TableInfo ti) {
    //     // Sqlserver 仅支持一个表设置 IDENTITY_INSERT ON
    //     // 当有其他表被设置的时候，需要off
    //     // 否则会抛出类似如下错误：表 'sakila.dbo.film' 的 IDENTITY_INSERT 已经为 ON。无法为表 'inventory' 执行 SET 操作。
    //     // 启用自动增长。
    //     // expect: set IDENTITY_INSERT <table> off;
    //
    //     try{
    //         super.enableIdentity(ti);
    //         System.out.println("enableIdentity, ti:" + ti.getTableName() + ", " + Thread.currentThread().getName());
    //     } catch (Exception e){
    //         e.printStackTrace();
    //         // if(e.getMessage() != null && e.getMessage().matches(".*IDENTITY_INSERT.*OFF.*")){
    //         //     String stmt = this.getReverseIdentityInsert(e.getMessage());
    //         //     if(stmt != null){
    //         //         this.getDatabaseDao().execute(stmt);
    //         //         super.enableIdentity(ti);
    //         //     }
    //         // } else {
    //         //     e.printStackTrace();
    //         // }
    //     } finally {
    //         this.identityInsertReentrantLock.unlock();
    //     }
    // }
    //
    // @Override
    // public void disableIdentity(TableInfo ti) {
    //     // Sqlserver 仅支持一个表设置 IDENTITY_INSERT ON
    //     // 否则会抛出类似如下错误：表 'sakila.dbo.film' 的 IDENTITY_INSERT 已经为 OFF。无法为表 'inventory' 执行 SET 操作。
    //     // 禁用自动增长。
    //     // expect: set IDENTITY_INSERT <table> on;
    //     try{
    //         // 将表名添加到队列中，因为只有一个表可以设置为on
    //         this.identityInsertReentrantLock.lock();
    //         super.disableIdentity(ti);
    //         System.out.println("disableIdentity, ti:" + ti.getTableName() + ", " + Thread.currentThread().getName());
    //     } catch (Exception e){
    //         e.printStackTrace();
    //         // if(e.getMessage() != null && e.getMessage().matches(".*IDENTITY_INSERT.*ON.*")){
    //         //     String stmt = this.getReverseIdentityInsert(e.getMessage());
    //         //     if(stmt != null){
    //         //         this.getDatabaseDao().execute(stmt);
    //         //         super.disableIdentity(ti);
    //         //     }
    //         // } else {
    //         //     e.printStackTrace();
    //         // }
    //     }
    // }
    //
    // private String getReverseIdentityInsert(String message){
    //     for(String s: message.split(";")){
    //         if(!s.contains("IDENTITY_INSERT")){
    //             continue;
    //         }
    //         s = s.substring(s.indexOf("'") + 1);
    //         if(!s.contains("ON")) {
    //             // 构建off
    //             return "set IDENTITY_INSERT " + s.substring(0, s.indexOf("'")) + " OFF";
    //         } else if(!s.contains("OFF")){
    //             // 构建on
    //             return "set IDENTITY_INSERT " + s.substring(0, s.indexOf("'")) + " ON";
    //         }
    //     }
    //     return null;
    // }
}
