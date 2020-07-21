package com.airwallex.data.streaming.common.extractor;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

public class RowTimestampAssigner implements SerializableTimestampAssigner<Row> {

    private final int timestampIndex;
    private final TypeInformation<?> fieldType;

    public RowTimestampAssigner(int timestampIndex, TypeInformation<?> fieldType) {
        this.fieldType = fieldType;
        this.timestampIndex = timestampIndex;
    }

    @Override
    public long extractTimestamp(Row element, long recordTimestamp) {
        Object time = element.getField(timestampIndex);
        if (fieldType.getTypeClass().isAssignableFrom(Timestamp.class)) {
            return ((Timestamp) time).getTime();
        } else {
            if (time == null)
                return 0L;
            return (Long) time;
        }
    }
}
