package com.liaobaikai.ngoxdb.core.converter.impl;

import com.liaobaikai.ngoxdb.bean.NgoxDbMaster;
import com.liaobaikai.ngoxdb.bean.info.ColumnInfo;
import com.liaobaikai.ngoxdb.bean.info.ConstraintInfo;
import com.liaobaikai.ngoxdb.bean.info.IndexInfo2;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.bean.rs.ImportedKey;
import com.liaobaikai.ngoxdb.core.comparator.DatabaseComparator;
import com.liaobaikai.ngoxdb.core.comparator.impl.OracleDatabaseComparator;
import com.liaobaikai.ngoxdb.core.converter.BasicDatabaseConverter;
import com.liaobaikai.ngoxdb.core.dao.BasicDatabaseDao;
import com.liaobaikai.ngoxdb.core.dao.impl.OracleDatabaseDao;
import com.liaobaikai.ngoxdb.core.dialect.DatabaseDialect;
import com.liaobaikai.ngoxdb.core.dialect.impl.OracleDatabaseDialect;
import com.liaobaikai.ngoxdb.core.entity.NgoxDbRelayLog;
import com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum;
import com.liaobaikai.ngoxdb.utils.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.Types;
import java.util.List;

/**
 * Oracle数据库转换器
 * <p>
 * https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/sql-language-reference.pdf
 *
 * @author baikai.liao
 * @Time 2021-01-31 23:31:25
 */
@Slf4j
@Service
public class OracleDatabaseConverter extends BasicDatabaseConverter {

    private final OracleDatabaseDao databaseDao;
    private final OracleDatabaseComparator databaseComparator;

    public OracleDatabaseConverter() {
        this.databaseDao = null;
        this.databaseComparator = null;
    }

    public OracleDatabaseConverter(NgoxDbMaster ngoxDbMaster) {
        super(ngoxDbMaster);
        this.databaseDao = new OracleDatabaseDao(ngoxDbMaster);
        this.databaseComparator = new OracleDatabaseComparator(this);
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
        return OracleDatabaseDialect.class;
    }

    @Override
    public String getDatabaseVendor() {
        return DatabaseVendorEnum.ORACLE.getVendor();
    }

    // @Override
    // public void handleException(Throwable e) {
    //     if(e.getMessage() != null){
    //         if(e.getMessage().contains("ORA-01408")){
    //             // ignored.
    //             return;
    //         }
    //     }
    //
    //     super.handleException(e);
    // }

    @Override
    public void formatConstraintColumnName(ConstraintInfo ci, List<ColumnInfo> columnInfoList) {
        String checkCondition = ci.getCheckCondition();
        // 去掉特有的格式
        for (ColumnInfo columnInfo : columnInfoList) {
            checkCondition = checkCondition.replaceAll(
                    StringUtils.escape("\"" + columnInfo.getColumnName() + "\""), columnInfo.getColumnName());
        }
        ci.setCheckCondition(checkCondition);
    }

    @Override
    protected void buildForeignKeyChangeRules(StringBuilder sBuilder, ImportedKey importedKey) {

        // https://docs.oracle.com/cd/E11882_01/server.112/e41084/statements_3001.htm#CJAHHIBI
        // https://docs.oracle.com/cd/E11882_01/server.112/e41084/clauses002.htm#CJAFFBAA

        // oracle仅支持以下属性
        // [ [ [ NOT ] DEFERRABLE ]
        //   [ INITIALLY { IMMEDIATE | DEFERRED } ]
        // | [ INITIALLY { IMMEDIATE | DEFERRED } ]
        //   [ [ NOT ] DEFERRABLE ]
        // ]
        // [ RELY | NORELY ]
        // [ using_index_clause ]
        // [ ENABLE | DISABLE ]
        // [ VALIDATE | NOVALIDATE ]
        // [ exceptions_clause ]
    }

    // @Override
    // protected String buildIndexName(IndexInfo2 ii, TableInfo ti) {
    //     String identifier = super.buildIndexName(ii, ti);
    //     int maxIdentifierLength = this.getDatabaseDao().getMaxColumnNameLength();
    //     if(identifier.length() > maxIdentifierLength) {
    //         // java.sql.SQLSyntaxErrorException: ORA-00972: 标识符过长
    //         // ORA-00972: Identifier is too Long (Doc ID 1955166.1)
    //         // In an Oracle database, objects names have a maximum of 30 characters.
    //         // This is why the ORA-00972 error is returned: the generated Supplemental Log Group name (identifier) is too long.
    //         if (ii.isNonUnique()) {
    //             identifier = "idx_";
    //         } else {
    //             identifier = "ux_";
    //         }
    //
    //         if (identifier.length() + ti.getTableName().length() <= maxIdentifierLength) {
    //             identifier += ti.getTableName() + "_";
    //         }
    //
    //         identifier += UUID.randomUUID().toString().replaceAll("-", "").substring(0, maxIdentifierLength - identifier.length());
    //     }
    //     return identifier;
    // }

    // @Override
    // public String getDatabaseDataTypeDefault(String masterDataTypeDef, int dataType) {
    //     String defaultValue = super.getDatabaseDataTypeDefault(masterDataTypeDef, dataType);
    //     if (dataType == Types.BOOLEAN) {
    //         if ("true".equals(masterDataTypeDef)) {
    //             return "1";
    //         } else if ("false".equals(masterDataTypeDef)) {
    //             return "0";
    //         }
    //     }
    //     return defaultValue;
    // }

    @Override
    protected void afterBuildIndex(StringBuilder sBuilder, IndexInfo2 ii, TableInfo ti) {

        // 1, Normal indexes. (By default, Oracle Database creates B-tree indexes.)
        // 2, Bitmap indexes, which store rowids associated with a key value as a bitmap
        // 3, Partitioned indexes, which consist of partitions containing an entry for each value that appears in the indexed column(s) of the table
        // 4, Function-based indexes, which are based on expressions. They enable you to construct queries that evaluate the value returned by an expression, which in turn may include built-in or user-defined functions.
        // 5, Domain indexes, which are instances of an application-specific index of type indextype

        switch (ii.getType()) {
            case IndexInfo2.tableIndexFullText:
                // Domain indexes, 全文索引
                // basic_lexer
                // chinese_vgram_lexer
                // chinese_lexer(utf8)
                // 1, 需要创建语法分析器
                // 2, 使用语法分析器来定义index type
                // this.getLogger().error("[{}] BUILD: table: {}, indexName: {}, indexType: fullText.", this.getDatabaseConfig().getName(), ti.getTableName(), ii.getIndexName());
                sBuilder.append(" indextype is ctxsys.context ");
                // sBuilder.append(" indextype is ctxsys.context parameters('lexer <LEXER_NAME>')");
                // this.getLogger().error("[{}] BUILD: expect: {}", this.getDatabaseConfig().getName(), sBuilder.toString());
                // this.getLogger().error("[{}] BUILD: expect execute statement use DBA roles. ", this.getDatabaseConfig().getName());
                // sBuilder.delete(0, sBuilder.length());
                break;
            case IndexInfo2.tableIndexPartitioned:
                // 分区索引
                break;
            case IndexInfo2.tableIndexDomain:
                // 域索引
                break;
            case IndexInfo2.tableIndexFunction:
                // 函数索引
                break;
        }

        sBuilder.append(" nologging");
    }

    @Override
    public void beforeImportRows(Connection con, List<Object[]> batchArgs, TableInfo ti) {

        // oracle的空值和NULL同一个意思。因此添加not null约束的时候，'' 和 NULL均会抛出 ora-01400
        // 因此，对于not null的字段，且传入的值是 ''，需要替换成 ' '，NULL的值需要替换为默认值。
        for (Object[] objects : batchArgs) {
            // row
            for (int x = 0, len = objects.length; x < len; x++) {
                // cell
                ColumnInfo columnInfo = ti.getColumns().get(x);
                if (!columnInfo.isNotNull()) {
                    // enable null
                    continue;
                }
                // 下面的类型需要判断。
                switch (columnInfo.getDataType()) {
                    case Types.VARCHAR:
                    case Types.NVARCHAR:

                    case Types.CHAR:
                    case Types.NCHAR:

                    case Types.CLOB:
                    case Types.NCLOB:

                    case Types.LONGVARCHAR:
                    case Types.LONGNVARCHAR:
                        if (objects[x] == null) {
                            // 使用默认值
                            objects[x] = columnInfo.getColumnDef();
                        } else if ("".equals(objects[x])) {
                            // 空字符串
                            objects[x] = " ";
                        }
                        break;
                }

            }
        }
    }


    @Override
    public String buildCreateTable(TableInfo ti) {
        return super.buildCreateTable(ti) + " nologging";
    }

    @Override
    public void afterCreateTable(TableInfo ti) {
        super.afterCreateTable(ti);

        // 将表设置为logging;
        this.getDatabaseDao().insertLogRows(new NgoxDbRelayLog(getFinalTableName(ti.getTableName()), NgoxDbRelayLog.TYPE_OTHER,
                String.format("alter table %s logging", this.toLookupName(ti.getTableName()))));
    }

    @Override
    protected void bindColumnType(String tableName, TableInfo ti) {
        super.bindColumnType(tableName.toUpperCase(), ti);
    }

    @Override
    public DatabaseComparator getComparator() {
        return this.databaseComparator;
    }

}
