package com.airwallex.data.streaming.common.rowformat;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.util.Map;

public abstract class RowFormatHelper {
    RowTypeInfo rowTypeInfo;

    public RowTypeInfo getRowTypeInfo() {
        return rowTypeInfo;
    }

    public DeserializationSchema<Row> getValueDeserializer() {
        return valueDeserializer;
    }

    DeserializationSchema<Row> valueDeserializer = null;

    public RowFormatHelper(Map<String, String> properties) {
        this.rowTypeInfo = buildRowTypeInfo(properties);
        this.valueDeserializer = buildDeserializationSchema(this.rowTypeInfo, properties);
    }

    protected abstract RowTypeInfo buildRowTypeInfo(Map<String, String> properties);

    protected abstract DeserializationSchema<Row> buildDeserializationSchema(RowTypeInfo rowTypeInfo, Map<String, String> properties);


}
