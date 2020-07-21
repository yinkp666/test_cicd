package com.airwallex.data.streaming.common.extractor;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

/**
 * @Depracted Because flink 1.11 version is using TimestampAssigner instead of TimestampExtractor
 */
@Deprecated
public class RowAscendingTimestampExtractor extends AscendingTimestampExtractor<Row> {

    int timeStampIndex = 0;
    TypeInformation<?> fieldType;
    private Logger logger = LoggerFactory.getLogger(RowAscendingTimestampExtractor.class);

    public RowAscendingTimestampExtractor(int timeStampIndex, TypeInformation<?> fieldType) {

        this.timeStampIndex = timeStampIndex;
        this.fieldType = fieldType;
        logger.info("Create Row Ascending Extractor for timestampIndex : "+timeStampIndex);

    }

    @Override
    public long extractAscendingTimestamp(Row element) {
        Object time = element.getField(timeStampIndex);
        if (fieldType.getTypeClass().isAssignableFrom(Timestamp.class)) {
            return ((Timestamp) time).getTime();
        } else {
            if (time == null)
                return 0L;
            return (Long) time;
        }
    }
}
