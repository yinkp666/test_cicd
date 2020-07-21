package com.airwallex.data.streaming.common.dimension;

import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.NavigableMap;

public class BigtableFunc extends AbstractTableFunc {
    private static final long serialVersionUID = -1408840130246375742L;

    private transient Table table;

    private transient Connection connection;

    String tableName;

    private BigtableFunc(Builder builder) {
        super(builder.fieldNames, builder.fieldTypes, builder.joinKeyNames,
                builder.isCached, builder.cacheType, builder.cacheMaxSize, builder.cacheExpireMs,
                builder.maxRetryTimes);
        this.tableName = builder.tableName;
    }


    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        initCache();
        Configuration config = HBaseConfiguration.create();

        connection = ConnectionFactory.createConnection(config);
        table = connection.getTable(TableName.valueOf(this.tableName));

    }

    @Override
    Row getRow(Object rowKey) {

        Get get = new Get(Bytes.toBytes((String) rowKey));

        Object[] filedValue = new Object[fieldNames.length];

        try {
            Result result = table.get(get);

            NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes("p"));

            String mid = null;
            String mail = null;
            for (byte[] k : map.keySet()) {
                String key = Bytes.toString(k);
                if (key.startsWith("merchant")) {
                    mid = Bytes.toString(map.get(k));
                } else if (key.startsWith("m_")) {
                    mail = Bytes.toString(map.get(k));
                }
            }
            filedValue[0] = rowKey;
            filedValue[1] = mid;
            filedValue[2] = mail;
            return Row.of(filedValue);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;

    }

    public void eval(Object... paramas) {
        String key = "t_"+(String) paramas[0];

        if (isCached) {
            if ("ALL".equals(cacheType) && cache instanceof LoadingCache) {
                LoadingCache<String, Row> loadingCache = (LoadingCache<String, Row>) cache;
                collect(loadingCache.get(key));
            } else {
                collect(cache.get(key, this::getRow));
            }
        } else {
            Row row = getRow(key);
            collect(row);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (cache != null) {
            cache = null;
        }
    }

    public static final class Builder {

        private String tableName;
        private String[] fieldNames;
        private TypeInformation[] fieldTypes;
        private String[] joinKeyNames;
        private boolean isCached;
        private String cacheType;
        private long cacheMaxSize;
        private long cacheExpireMs;
        private int maxRetryTimes;

        private Builder() {
        }

        public Builder withTableName(String val) {
            tableName = val;
            return this;
        }

        public Builder withFieldNames(String[] val) {
            fieldNames = val;
            return this;
        }

        public Builder withFieldTypes(TypeInformation[] val) {
            fieldTypes = val;
            return this;
        }

        public Builder withJoinKeyNames(String[] val) {
            joinKeyNames = val;
            return this;
        }

        public Builder withIsCached(boolean val) {
            isCached = val;
            return this;
        }

        public Builder withCacheType(String val) {
            cacheType = val;
            return this;
        }

        public Builder withCacheMaxSize(long val) {
            cacheMaxSize = val;
            return this;
        }

        public Builder withCacheExpireMs(long val) {
            cacheExpireMs = val;
            return this;
        }

        public Builder withMaxRetryTimes(int val) {
            maxRetryTimes = val;
            return this;
        }

        public BigtableFunc build() {
            return new BigtableFunc(this);
        }
    }
}
