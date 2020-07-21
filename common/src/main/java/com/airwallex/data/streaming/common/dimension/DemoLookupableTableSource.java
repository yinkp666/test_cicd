package com.airwallex.data.streaming.common.dimension;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

public class DemoLookupableTableSource implements LookupableTableSource<Row> {
    private final String[] fieldNames;
    private final TypeInformation[] fieldTypes;
    private final boolean isAsync;

    public DemoLookupableTableSource(String[] fieldNames, TypeInformation[] fieldTypes, boolean isAsync) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.isAsync = isAsync;
    }

    /**
     * 同步方法，这里你需要自定义 TableFuntion 进行实现
     * TableFunction 本质是 UDTF,输入一条数据可能返回多条数据，也可能返回一条数据
     *
     * @param strings a join b on b.id = a.user_id ,此时代表 id
     * @return org.apache.flink.table.functions.AsyncTableFunction<org.apache.flink.types.Row>
     * @author cuishilei
     * @date 2019/9/1
     */
    @Override
    public TableFunction<Row> getLookupFunction(String[] strings) {

        return BigtableFunc.newBuilder()
                .withJoinKeyNames(strings)
                .withFieldNames(fieldNames)
                .withFieldTypes(fieldTypes)
                .withCacheExpireMs(180)
                .withIsCached(true)
                .withCacheMaxSize(100)
                .withTableName("test_profile")
                .build();

    }

    /**
     * 异步方法，这里你需要自定义 TableFuntion 进行实现，这里与 异步io 是类似的。
     * TableFunction 本质是 UDTF,输入一条数据可能返回多条数据，也可能返回一条数据
     *
     * @param strings a join b on b.id = a.user_id ,此时代表 id
     * @return org.apache.flink.table.functions.AsyncTableFunction<org.apache.flink.types.Row>
     * @author cuishilei
     * @date 2019/9/1
     */
    @Override
    public AsyncTableFunction<Row> getAsyncLookupFunction(String[] strings) {
        return null;
    }

    /**
     * 声明表的源数据获取方法，如果想使用 getAsyncLookupFunction 则应返回 true
     *
     * @return boolean
     * @author cuishilei
     * @date 2019/9/1
     */
    @Override
    public boolean isAsyncEnabled() {
        return isAsync;
    }

    /**
     * 声明表的结构，TableSchema 中含有关于 字段名 字段类型的信息
     *
     * @return org.apache.flink.table.api.TableSchema
     * @author cuishilei
     * @date 2019/9/1
     */
    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder()
                .fields(fieldNames, TypeConversions.fromLegacyInfoToDataType(fieldTypes))
                .build();
    }

    /**
     * 声明表的返回列信息，此处没有深究如果写的比 TableSchema 少的话，会返回什么效果，
     * 是做过滤用还是必须和 TableSchema 保持一致未知，知道的大佬可以评论和我说一下
     *
     * @return org.apache.flink.api.common.typeinfo.TypeInformation<org.apache.flink.types.Row>
     * @author cuishilei
     * @date 2019/9/1
     */
    @Override
    public TypeInformation<Row> getReturnType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }
}
