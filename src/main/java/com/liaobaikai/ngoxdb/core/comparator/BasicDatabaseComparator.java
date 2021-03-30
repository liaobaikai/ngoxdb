package com.liaobaikai.ngoxdb.core.comparator;

import com.liaobaikai.ngoxdb.bean.ComparisonResult;
import com.liaobaikai.ngoxdb.bean.info.TableInfo;
import com.liaobaikai.ngoxdb.core.converter.DatabaseConverter;
import com.liaobaikai.ngoxdb.core.enums.DatabaseVendorEnum;
import com.liaobaikai.ngoxdb.core.listener.OnComparingListener;
import com.liaobaikai.ngoxdb.utils.DateUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 基础数据库比较器
 *
 * @author baikai.liao
 * @Time 2021-03-04 14:31:59
 */
public abstract class BasicDatabaseComparator implements DatabaseComparator {

    /**
     * 比较器比较结果
     */
    public List<ComparisonResult> comparisonResultList;

    private final DatabaseConverter databaseConverter;

    public BasicDatabaseComparator(DatabaseConverter databaseConverter) {
        this.databaseConverter = databaseConverter;
        this.comparisonResultList = new ArrayList<>();
    }

    @Override
    public DatabaseConverter getDatabaseConverter() {
        return this.databaseConverter;
    }

    public List<ComparisonResult> getComparisonResultList() {
        return comparisonResultList;
    }

    public ComparisonResult findComparisonResult(String tableName) {
        for (ComparisonResult comparisonResult : this.comparisonResultList) {
            if (comparisonResult.getSlaveName().equals(this.getDatabaseConverter().getDatabaseConfig().getName())
                    && comparisonResult.getTableName().equals(tableName)) {
                return comparisonResult;
            }
        }
        return null;
    }

    @Override
    public void compareAllTables(List<TableInfo> tis, OnComparingListener onComparingListener) {
        for (TableInfo ti : tis) {
            // 查询表的所有数据

            final String table = ti.getTableName();

            // 迁移表的数据
            // 查询原系统的数据
            int pageSize = this.getDatabaseConverter().getDatabaseConfig().getPageSize();
            // 获取表的行数
            long tableRowCount = this.getDatabaseConverter().getDatabaseDao().getTableRowCount(table);
            if (tableRowCount == 0) {
                // 无数据
                if (onComparingListener != null) {
                    onComparingListener.onComparing(ti, tableRowCount, 0, pageSize, new ArrayList<>());
                }
                continue;
            }

            // 分页查询
            // 计算表的页数
            long tablePageSize = (tableRowCount / pageSize) + (tableRowCount % pageSize > 0 ? 1 : 0);

            for (int i = 0, offset, limit; i < tablePageSize; i++) {
                // 循环获取每一个分页的数据
                offset = i * pageSize;
                limit = (i != tablePageSize - 1) ? pageSize : (int) (tableRowCount - (long) offset);

                List<Object[]> batchArgs = this.getDatabaseConverter().pagination(ti, offset, limit);

                if (onComparingListener != null) {
                    onComparingListener.onComparing(ti, tableRowCount, 0, limit, batchArgs);
                }
            }

        }
    }

    @Override
    public ComparisonResult compare(TableInfo ti,
                                    List<Object[]> sourceArgs,
                                    List<Object[]> targetArgs) {
        return this.compare(ti, sourceArgs, targetArgs, -1);
    }

    @Override
    public ComparisonResult compare(TableInfo ti,
                                    List<Object[]> sourceArgs,
                                    List<Object[]> targetArgs,
                                    int uniqueKeyIndex) {

        int sourceCount = sourceArgs.size();
        int diff = 0;
        int rowDiff = 0;

        // 检查行信息
        for (int i = 0; i < sourceCount; i++) {
            Object[] sr = sourceArgs.get(i);
            Object[] tr;
            if (uniqueKeyIndex >= 0) {
                tr = this.findRowArgsByIndex(targetArgs, sr[uniqueKeyIndex], uniqueKeyIndex);
                if (tr == null) {
                    rowDiff++;
                    System.out.println("RowDiff0: rowIndex: " + i + ", sourceValue: " + sr[0]);
                    continue;
                }
            } else {
                tr = targetArgs.get(i);
            }
            for (int j = 0; j < sr.length; j++) {
                // 两个都是空的，正确
                Object sv = sr[j];
                Object tv = tr == null ? null : tr[j];

                // 源端是null，通过
                if (sv == null) {
                    continue;
                }

                // oracle数据库null和''结果一样。
                if (DatabaseVendorEnum.isOracle(this.getDatabaseConverter().getDatabaseConfig().getDatabase())) {
                    if (tv == null || tv.toString().length() == 0) {
                        // oracle database null is ''
                        continue;
                    }
                }

                if (tv == null) {
                    System.out.println("Diff0: rowIndex: " + i + ", colIndex: " + j + ", sourceValue: " + sv + ", targetValue: null");
                    continue;
                }

                // 这些类型都可以转成BigDecimal。
                BigDecimal bd1 = toBigDecimal(sv), bd2 = toBigDecimal(tv);

                if (bd1 != null && bd2 != null && bd1.compareTo(bd2) == 0) {
                    // 正确
                    continue;
                } else if (bd1 != null && bd2 != null && bd1.compareTo(bd2) != 0) {
                    // 不正确
                    diff++;
                    System.out.println("Diff1: rowIndex: " + i + ", colIndex: " + j + ", sourceValue: " + sv + ", targetValue: " + tv);
                    continue;
                }

                if (sv instanceof Timestamp) {
                    // sourceValue: 2017-06-06 19:49:51.453, targetValue: 2017-06-06 19:49:51.0
                    // MySQL不支持毫秒级别。
                    Timestamp timestamp1 = DateUtils.stripMills((Timestamp) sv);
                    Timestamp timestamp2 = DateUtils.stripMills((Timestamp) tv);
                    // 时间一样吗
                    if (timestamp1.getTime() != timestamp2.getTime()) {
                        System.out.println("Diff3: rowIndex: " + i + ", colIndex: " + j + ", sourceValue: " + sv + ", targetValue: " + tv);
                    }
                    continue;
                }
                // 字符直接判断
                if (!sv.equals(tv)) {
                    diff++;
                    System.out.println("Diff2: rowIndex: " + i + ", colIndex: " + j + ", sourceValue: " + sv + ", targetValue: " + tv);
                }
            }
        }

        // 查找表是否有对比的结果
        ComparisonResult comparisonResult = this.findComparisonResult(ti.getTableName());
        if (comparisonResult == null) {
            comparisonResult = new ComparisonResult();
            comparisonResult.setPages(1);
            comparisonResult.setSlaveName(this.getDatabaseConverter().getDatabaseConfig().getName());
            comparisonResult.setRows(sourceCount);
            comparisonResult.setDiffs(diff);
            comparisonResult.setFinishTime(new Date());
            comparisonResult.setTableName(ti.getTableName());
            comparisonResult.setDiffRows(rowDiff);
            this.getComparisonResultList().add(comparisonResult);
        } else {
            comparisonResult.setDiffs(comparisonResult.getDiffs() + diff);
            comparisonResult.setDiffRows(comparisonResult.getDiffRows() + rowDiff);
            comparisonResult.setRows(comparisonResult.getRows() + sourceCount);
            comparisonResult.setPages(comparisonResult.getPages() + 1);
        }

        return comparisonResult;
    }

    /**
     * 查找数据
     *
     * @param targetArgs     目标数据库返回的每行数据
     * @param value          需要查询的值
     * @param uniqueKeyIndex 唯一键的索引
     * @return 查询到的行信息
     */
    private Object[] findRowArgsByIndex(List<Object[]> targetArgs, Object value, int uniqueKeyIndex) {

        for (Object[] objects : targetArgs) {
            if (value != null && value.equals(objects[uniqueKeyIndex])) {
                return objects;
            }
        }
        return null;
    }

    /**
     * 将数字类型转换为BigDecimal
     *
     * @param value 列的值
     * @return BigDecimal对象
     */
    private static BigDecimal toBigDecimal(Object value) {
        BigDecimal bd = null;
        if (value instanceof BigDecimal) {
            // oracle -> BigDecimal
            // access -> int
            bd = (BigDecimal) value;
        } else if (value instanceof Boolean) {
            bd = new BigDecimal(((Boolean) value) ? "1" : "0");
        } else if (value instanceof Short
                || value instanceof Integer
                || value instanceof Long
                || value instanceof Float
                || value instanceof Double
                || value instanceof BigInteger) {
            bd = new BigDecimal(value.toString());
        }
        return bd;
    }
}
