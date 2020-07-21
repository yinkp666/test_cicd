package com.airwallex.data.streaming.common.factory;

import com.airwallex.data.streaming.common.config.FlinkConfig;
import com.airwallex.data.streaming.common.rowformat.RowFormatHelper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class KafkaSourceFactory {
    public static DataStream<Row> registerSource(StreamTableEnvironment bsTableEnv, StreamExecutionEnvironment bsEnv, FlinkConfig.KafkaSourceConfig kafkaSourceConfig){
        FlinkConfig.FormatConfig formatConfig = kafkaSourceConfig.getFormatConfig();

        RowFormatHelper rowFormatHelper = RowFormatHelperFactory.createFormatHelper(formatConfig);

        DeserializationSchema<Row> valueDeserializer = rowFormatHelper.getValueDeserializer();

        RowTypeInfo rowTypeInfos = rowFormatHelper.getRowTypeInfo();

        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<>(
                kafkaSourceConfig.getTopic(),
                valueDeserializer,
                kafkaSourceConfig.getProperties()
        );

        String rowTime = kafkaSourceConfig.getRowtime();

        Boolean isKeyedStream = kafkaSourceConfig.getKeyedStream();

        String keyField = kafkaSourceConfig.getKeyField();

        long slideWindowSize = kafkaSourceConfig.getSlideWindowSize();

        DataStream<Row> stream = bsEnv.addSource(consumer);

        return stream;

    }
}
