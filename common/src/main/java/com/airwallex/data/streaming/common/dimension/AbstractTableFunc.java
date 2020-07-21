package com.airwallex.data.streaming.common.dimension;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class AbstractTableFunc extends TableFunction<Row> {
    private static final long serialVersionUID = -1272426765940357679L;
    /**
     * 返回 row 的字段名
     */
    protected final String[] fieldNames;
    /**
     * 返回 row 的字段类型
     */
    protected final TypeInformation[] fieldTypes;
    /**
     * join key的字段名
     */
    protected final String[] joinKeyNames;
    protected final boolean isCached;
    protected final String cacheType;
    protected final long cacheMaxSize;
    protected final long cacheExpireMs;
    protected final int maxRetryTimes;

    /**
     * join key的索引
     */
    protected int[] joinKeyIndexes;
    /**
     * 非 join key 的字段
     */
    protected String[] otherFieldNames;
    /**
     * 非 join key 的字段索引
     */
    protected int[] otherFieldNamesIndexes;

    /**
     * 缓存
     */
    protected transient Cache<String, Row> cache;

    public AbstractTableFunc(String[] fieldNames, TypeInformation[] fieldTypes, String[] joinKeyNames,
                             boolean isCached, String cacheType, long cacheMaxSize, long cacheExpireMs,
                             int maxRetryTimes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.joinKeyNames = joinKeyNames;
        this.isCached = isCached;
        this.cacheType = cacheType;
        this.cacheMaxSize = cacheMaxSize;
        this.cacheExpireMs = cacheExpireMs;
        this.maxRetryTimes = maxRetryTimes;
        List<String> fieldNamesList = Arrays.asList(fieldNames);
        //找出主键索引
        joinKeyIndexes = getFieldIndexes(fieldNames, joinKeyNames);
        //找出非主键的字段的索引
        otherFieldNames = Sets.difference(Sets.newHashSet(fieldNames), Sets.newHashSet(joinKeyNames))
                .toArray(new String[]{});
        otherFieldNamesIndexes = getFieldIndexes(fieldNames, otherFieldNames);
    }

    private int[] getFieldIndexes(String[] fieldNames, String[] otherFieldNames) {
        return joinKeyIndexes;
    }

    protected void initCache() {
        if (isCached) {
            if ("ALL".equals(cacheType)) {
                //全表缓存用 LoadingCache
                cache = Caffeine.newBuilder()
                        .refreshAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                        .build(new CacheLoader<String, Row>() {
                            @Nullable
                            @Override
                            public Row load(@NonNull String s) {
                                return getRow(s);
                            }
                        });
            } else {
                //非全表缓存用 Cache
                cache = Caffeine.newBuilder()
                        .maximumSize(cacheMaxSize)
                        .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                        .build();
            }
        }
    }

    /**
     * 返回需要的 row
     *
     * @param data 定制参数
     * @return org.apache.flink.types.Row
     * @author cuishilei
     * @date 2019/9/1
     */
    abstract Row getRow(Object data);

    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }
}
