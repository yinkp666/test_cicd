package com.airwallex.data.streaming.common.rowformat;

import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author jim.yang
 */
public class TupleRowToJsonKafkaSerializationSchema implements KafkaSerializationSchema<Tuple2<Boolean, Row>> {
    /**
     * Object mapper for parsing the JSON.
     */
    private final ObjectMapper objectMapper = new ObjectMapper();
    /**
     * Reusable object node.
     */
    private transient ObjectNode connectNode;

    private String topic;
    private int keyIndex;
    RowTypeInfo typeInfo;

    /**
     * @param typeInfo
     */
    public TupleRowToJsonKafkaSerializationSchema(RowTypeInfo typeInfo, String topic, int keyIndex) {
        this.typeInfo = typeInfo;
        this.topic = topic;
        this.keyIndex = keyIndex;
    }

    private void initConnectNode() {

        try {
            connectNode = this.objectMapper.createObjectNode();
            String schemaTemplate = "{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"retractFlag\"}],\"optional\":false,\"name\":\"json-schema\"}";
            ObjectNode schemaNode = (ObjectNode) this.objectMapper.readTree(schemaTemplate);
            ArrayNode arrayNode = (ArrayNode) schemaNode.get("fields");
            for (String field : typeInfo.getFieldNames()) {
                ObjectNode fieldNode = objectMapper.createObjectNode();
                fieldNode.put("type", "string");
                fieldNode.put("field", field);
                fieldNode.put("optional", true);
                arrayNode.add(fieldNode);
            }
            connectNode.put("schema", schemaNode);
        } catch (IOException e) {

        }
    }

    public static void main(String[] args) {
       new TupleRowToJsonKafkaSerializationSchema( null," String topic", 1).initConnectNode();
    }


    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            Tuple2<Boolean, Row> element, @Nullable Long timestamp
    ) {
        if (connectNode == null) {
            initConnectNode();
        }
        try {
            ObjectNode payload = objectMapper.createObjectNode();
            payload.put("retractFlag", String.valueOf(element.f0));

            for (String field : typeInfo.getFieldNames()) {
                Object v = element.f1.getField(typeInfo.getFieldIndex(field));
                payload.put(field, String.valueOf(v));
            }
            connectNode.put("payload", payload);

            byte[] value = objectMapper.writeValueAsBytes(connectNode);

            return new ProducerRecord<byte[], byte[]>(topic,
                    (element.f1.getField(keyIndex)).toString().getBytes(),
                    value
            );
        } catch (Throwable t) {
            throw new RuntimeException("Could not serialize row '" + element + "'", t);
        } finally {
            connectNode.remove("payload");
        }
    }
}
