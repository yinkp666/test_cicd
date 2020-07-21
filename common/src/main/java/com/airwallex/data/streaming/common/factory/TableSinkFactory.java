package com.airwallex.data.streaming.common.factory;

import com.airwallex.data.streaming.common.config.FlinkConfig;
import com.airwallex.data.streaming.common.rowformat.JsonRowKafkaSerializationSchema;
import com.airwallex.data.streaming.common.rowformat.TupleRowToJsonKafkaSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class TableSinkFactory {

    public static SinkFunction<Row> createAppendSink(Properties properties, FlinkConfig.SinkInstance sinkInstance) {
        if (sinkInstance.getType().equals("kafka")) {
            String topic = properties.getProperty("topic");
            int keyIndex = (int) properties.getOrDefault("keyIndex", 0);
            FlinkKafkaProducer myProducer = new FlinkKafkaProducer(topic,
                    (KafkaSerializationSchema<Row>) (element, timestamp) -> new ProducerRecord<byte[], byte[]>(topic,
                            (element.getField(keyIndex)).toString().getBytes(),
                            (element.toString()).getBytes()
                    ),
                    sinkInstance.getProps(),
                    FlinkKafkaProducer.Semantic.NONE
            );
            return myProducer;
        } else {
            return null;
        }
    }

    public static SinkFunction<Tuple2<Boolean, Row>> createRetractSink(
            Properties properties, FlinkConfig.SinkInstance sinkInstance
    ) {
        if (sinkInstance.getType().equals("kafka")) {
            String topic = properties.getProperty("topic");
            FlinkKafkaProducer.Semantic semantic =
                    FlinkKafkaProducer.Semantic.valueOf(properties.getProperty("semantic"));
            int keyIndex = (int) properties.getOrDefault("keyIndex", 0);

            FlinkKafkaProducer myProducer = new FlinkKafkaProducer(topic,
                    (KafkaSerializationSchema<Tuple2<Boolean, Row>>) (element, timestamp) -> new ProducerRecord<byte[], byte[]>(
                            topic,
                            (element.f1.getField(keyIndex)).toString().getBytes(),
                            (element.f0 + "," + element.f1.toString()).getBytes()
                    ),
                    sinkInstance.getProps(),
                    semantic
            );
            return myProducer;
        } else {
            return null;
        }
    }

    public static SinkFunction<Tuple2<Boolean, Row>> createJsonRetractSink(
            Properties properties,
            FlinkConfig.SinkInstance sinkInstance,
            TypeInformation<Tuple2<Boolean, Row>> typeInformation
    ) {

        TupleTypeInfo<Tuple2<Boolean, Row>> types = (TupleTypeInfo<Tuple2<Boolean, Row>>) typeInformation;
        TypeInformation<?> rowTypeInfo = types.getTypeAt(1);
        RowTypeInfo typeInfo = (RowTypeInfo) rowTypeInfo;

        if (sinkInstance.getType().equals("kafka")) {
            String topic = properties.getProperty("topic");
            FlinkKafkaProducer.Semantic semantic =
                    FlinkKafkaProducer.Semantic.valueOf(properties.getProperty("semantic"));
            int keyIndex = (int) properties.getOrDefault("keyIndex", 0);
            TupleRowToJsonKafkaSerializationSchema tupleRowToJsonKafkaSerializationSchema =
                    new TupleRowToJsonKafkaSerializationSchema(typeInfo, topic, keyIndex);

            return new FlinkKafkaProducer(topic,
                    tupleRowToJsonKafkaSerializationSchema,
                    sinkInstance.getProps(),
                    semantic
            );
        } else {
            return null;
        }
    }

    public static SinkFunction<Row> createJsonAppendSink(
            Properties properties, FlinkConfig.SinkInstance sinkInstance, TypeInformation<Row> typeInformation
    ) {
        if (sinkInstance.getType().equals("kafka")) {

            RowTypeInfo typeInfo = (RowTypeInfo) typeInformation;
            String topic = properties.getProperty("topic");
            int keyIndex = (int) properties.getOrDefault("keyIndex", 0);
            JsonRowKafkaSerializationSchema jsonRowKafkaSerializationSchema =
                    new JsonRowKafkaSerializationSchema(typeInfo, topic, keyIndex);
            FlinkKafkaProducer myProducer = new FlinkKafkaProducer(topic,
                    jsonRowKafkaSerializationSchema,
                    sinkInstance.getProps(),
                    FlinkKafkaProducer.Semantic.NONE
            );
            return myProducer;
        } else {
            return null;
        }
    }
}
