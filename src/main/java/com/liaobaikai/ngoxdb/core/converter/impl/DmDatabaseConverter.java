package com.liaobaikai.ngoxdb.core.converter.impl;

import com.liaobaikai.ngoxdb.bean.NgoxDbMaster;
import com.liaobaikai.ngoxdb.core.comparator.DatabaseComparator;
import com.liaobaikai.ngoxdb.core.converter.BasicDatabaseConverter;
import com.liaobaikai.ngoxdb.core.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.core.dao.impl.DmDatabaseDao;
import com.liaobaikai.ngoxdb.core.dialect.DatabaseDialect;
import com.liaobaikai.ngoxdb.core.dialect.impl.DmDatabaseDialect;
import com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

/**
 * 达梦数据库转换器
 *
 * @author baikai.liao
 * @Time 2021-03-11 16:09:25
 */
@Slf4j
@Service
public class DmDatabaseConverter extends BasicDatabaseConverter {

    private final DmDatabaseDao databaseDao;

    public DmDatabaseConverter() {
        this.databaseDao = null;
    }

    public DmDatabaseConverter(NgoxDbMaster ngoxDbMaster) {
        super(ngoxDbMaster);
        this.databaseDao = new DmDatabaseDao(ngoxDbMaster);
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
        return DmDatabaseDialect.class;
    }

    @Override
    public String getDatabaseVendor() {
        return DatabaseVendorEnum.DM.getVendor();
    }

    @Override
    public DatabaseComparator getComparator() {
        return null;
    }

    // @Override
    // public void importRows(List<Object[]> batchArgs, TableInfo ti, int offset, int limit) {

    // boolean hasAutoIdentity = this.hasAutoIdentity(ti);
    //
    // try {
    //     // 1.IDENTITY_INSERT 属性的默认值为 OFF。SET IDENTITY_INSERT 的设置是在 执行或运行时进行的。当一个连接结束，IDENTITY_INSERT 属性将被自动还原为 OFF;
    //     // 2.DM 要求一个会话连接中只有一个表的 IDENTITY_INSERT 属性可以设置为 ON， 当设置一个新的表 IDENTITY_INSERT 属性设置为 ON 时，之前已经设置为 ON 的表会自 动还原为 OFF。
    //     //      当一个表的 IDENTITY_INSERT 属性被设置为 ON 时，该表中的自动增量 列的值由用户指定。如果插入值大于表的当前标识值(自增列当前值)，则 DM 自动将新插入 值作为当前标识值使用，即改变该表的自增列当前值;否则，将不影响该自增列当前值;
    //     // 3.当设置一个表的 IDENTITY_INSERT 属性为 OFF 时，新插入行中自增列的当前值 由系统自动生成，用户将无法指定;
    //     // 4.自增列一经插入，无法修改;
    //     // 5.手动插入自增列，除了将 IDENTITY_INSERT 设置为 ON，还要求在插入列表中明 确指定待插入的自增列列名。
    //     //      插入方式与非 IDENTITY 表是完全一样的。如果插入时，既不指定自增列名也不给自增列赋值，则新插入行中自增列的当前值由系统自动生成。
    //     if(hasAutoIdentity){
    //         if(ti.getTableName().equals(
    //                 IdentityInsertUtils.getAfterSetIfUndefined(ti.getTableName(), getDatabaseDao().getJdbcTemplate()))){
    //             // 可以继续导入数据
    //             super.importRows(batchArgs, ti, offset, limit);
    //
    //             IdentityInsertUtils.remove(getDatabaseDao().getJdbcTemplate());
    //             // 禁用标识符
    //             this.enableIdentity(ti);
    //             return;
    //         }
    //         // 等待锁
    //         this.identityInsertReentrantLock.lock();
    //     }
    //
    //     super.importRows(batchArgs, ti, offset, limit);
    //
    // } catch (Exception e){
    //     e.printStackTrace();
    // } finally {
    //     if(hasAutoIdentity){
    //         this.identityInsertReentrantLock.unlock();
    //
    //         IdentityInsertUtils.remove(getDatabaseDao().getJdbcTemplate());
    //         // 禁用标识符
    //         this.enableIdentity(ti);
    //     }
    // }

    // }
}
