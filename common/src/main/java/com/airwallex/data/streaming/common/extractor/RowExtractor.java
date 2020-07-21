package com.airwallex.data.streaming.common.extractor;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

/**
 * @Depracted Because flink 1.11 version is using TimestampAssigner instead of TimestampExtractor
 */
@Deprecated
public class RowExtractor extends BoundedOutOfOrdernessTimestampExtractor<Row> {

    int timeStampIndex = 0;
    TypeInformation<?> fieldType;

    public RowExtractor(Time maxOutOfOrderness, int timeStampIndex, TypeInformation<?> fieldType) {
        super(maxOutOfOrderness);
        this.timeStampIndex = timeStampIndex;
        this.fieldType = fieldType;
    }

    @Override
    public long extractTimestamp(Row row) {

        Object time = row.getField(timeStampIndex);
        if (fieldType.getTypeClass().isAssignableFrom(Timestamp.class)) {
            return ((Timestamp) time).getTime();
        } else {
            if (time == null)
                return 0L;
            return (Long) time;
        }
    }
}