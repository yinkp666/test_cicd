/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.airwallex.data.streaming.common.template;


import com.airwallex.data.streaming.common.rowformat.MyAvroRowDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaStreamsJoin {


  public static void main(String[] args) throws Exception {
    // create execution environment

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().setAutoWatermarkInterval(1000);
//    KafkaAvroDecoder avroDecoder;

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "data-nonprod-1:9092");
    properties.setProperty("group.id", "flink_consumer");
    properties.setProperty("auto.offset.reset", "earliest");
    properties.setProperty("zookeeper.connect", "data-nonprod-1:2181");


//    VerifiableProperties vProps = new VerifiableProperties(properties);
//    avroDecoder = new KafkaAvroDecoder(vProps);
//    Schema schema = avroDecoder.getById(1);

    String topic_a = "test.aaron1";
    String topic_b = "test.aaron2";

    String avroSchema_a = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"PaymentAttemptRequestDTO\",\n" +
            "  \"namespace\": \"com.airwallex.generics\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"event_id\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        \"int\"\n" +
            "      ],\n" +
            "      \"default\": null\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"event_type\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        \"string\"\n" +
            "      ],\n" +
            "      \"default\": null\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"payment_intent_id\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        \"string\"\n" +
            "      ],\n" +
            "      \"default\": null\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"payment_attempt_id\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        \"string\"\n" +
            "      ],\n" +
            "      \"default\": null\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"data\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        {\n" +
            "          \"type\": \"record\",\n" +
            "          \"name\": \"EventData\",\n" +
            "          \"namespace\": \"com.airwallex.generics.PaymentAttemptRequestDTO\",\n" +
            "          \"fields\": [\n" +
            "            {\n" +
            "              \"name\": \"object\",\n" +
            "              \"type\": [\n" +
            "                \"null\",\n" +
            "                {\n" +
            "                  \"type\": \"record\",\n" +
            "                  \"name\": \"DataObject\",\n" +
            "                  \"fields\": [\n" +
            "                    {\n" +
            "                      \"name\": \"paymentMethod\",\n" +
            "                      \"type\": [\n" +
            "                        \"null\",\n" +
            "                        \"string\"\n" +
            "                      ],\n" +
            "                      \"default\": null\n" +
            "                    },\n" +
            "                    {\n" +
            "                      \"name\": \"paymentInstrument\",\n" +
            "                      \"type\": [\n" +
            "                        \"null\",\n" +
            "                        {\n" +
            "                          \"type\": \"record\",\n" +
            "                          \"name\": \"Instrument\",\n" +
            "                          \"fields\": [\n" +
            "                            {\n" +
            "                              \"name\": \"billing\",\n" +
            "                              \"type\": [\n" +
            "                                \"null\",\n" +
            "                                {\n" +
            "                                  \"type\": \"record\",\n" +
            "                                  \"name\": \"DataBilling\",\n" +
            "                                  \"fields\": [\n" +
            "                                    {\n" +
            "                                      \"name\": \"customerId\",\n" +
            "                                      \"type\": [\n" +
            "                                        \"null\",\n" +
            "                                        \"string\"\n" +
            "                                      ],\n" +
            "                                      \"default\": null\n" +
            "                                    }\n" +
            "                                  ]\n" +
            "                                }\n" +
            "                              ],\n" +
            "                              \"default\": null\n" +
            "                            }\n" +
            "                          ]\n" +
            "                        }\n" +
            "                      ],\n" +
            "                      \"default\": null\n" +
            "                    }\n" +
            "                  ]\n" +
            "                }\n" +
            "              ],\n" +
            "              \"default\": null\n" +
            "            },\n" +
            "            {\n" +
            "              \"name\": \"error\",\n" +
            "              \"type\": [\n" +
            "                \"null\",\n" +
            "                {\n" +
            "                  \"type\": \"record\",\n" +
            "                  \"name\": \"DataError\",\n" +
            "                  \"fields\": [\n" +
            "                    {\n" +
            "                      \"name\": \"errorCodes\",\n" +
            "                      \"type\": [\n" +
            "                        \"null\",\n" +
            "                        {\n" +
            "                          \"type\": \"array\",\n" +
            "                          \"items\": {\n" +
            "                            \"type\": \"record\",\n" +
            "                            \"name\": \"ErrorCodes\",\n" +
            "                            \"fields\": [\n" +
            "                              {\n" +
            "                                \"name\": \"errorCode\",\n" +
            "                                \"type\": [\n" +
            "                                  \"null\",\n" +
            "                                  \"string\"\n" +
            "                                ],\n" +
            "                                \"default\": null\n" +
            "                              },\n" +
            "                              {\n" +
            "                                \"name\": \"processorErrorCode\",\n" +
            "                                \"type\": [\n" +
            "                                  \"null\",\n" +
            "                                  \"string\"\n" +
            "                                ],\n" +
            "                                \"default\": null\n" +
            "                              }\n" +
            "                            ]\n" +
            "                          },\n" +
            "                          \"java-class\": \"java.util.List\"\n" +
            "                        }\n" +
            "                      ],\n" +
            "                      \"default\": null\n" +
            "                    }\n" +
            "                  ]\n" +
            "                }\n" +
            "              ],\n" +
            "              \"default\": null\n" +
            "            }\n" +
            "          ]\n" +
            "        }\n" +
            "      ],\n" +
            "      \"default\": null\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"created_at\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        {\n" +
            "          \"type\": \"long\",\n" +
            "          \"logicalType\": \"timestamp-millis\"\n" +
            "        }\n" +
            "      ],\n" +
            "      \"default\": null\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    String avroSchema_b = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"AttemptSimpleDTO\",\n" +
            "  \"namespace\": \"com.airwallex.generics\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"event_id\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        \"int\"\n" +
            "      ],\n" +
            "      \"default\": null\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"ts\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        {\n" +
            "          \"type\": \"long\",\n" +
            "          \"logicalType\": \"timestamp-millis\"\n" +
            "        }\n" +
            "      ],\n" +
            "      \"default\": null\n" +
            "    }\n" +
            "  ]\n" +
            "}";
//    String avroSchema_c = "{\n" +
//            "  \"type\": \"record\",\n" +
//            "  \"name\": \"SimpleDTO\",\n" +
//            "  \"namespace\": \"com.airwallex.generics\",\n" +
//            "  \"fields\": [\n" +
//            "    {\n" +
//            "      \"name\": \"event_id\",\n" +
//            "      \"type\": \"int\",\n" +
//            "      \"default\": 9999\n" +
//            "    }\n" +
//            "  ]\n" +
//            "}";
    EnvironmentSettings settings = EnvironmentSettings.newInstance()
            .useBlinkPlanner()
            .inStreamingMode()
            .build();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

    TypeInformation<Row> typeInfo = AvroSchemaConverter.convertToTypeInfo(avroSchema_a);
    System.out.println(typeInfo);
    RowTypeInfo rowTypeInfo = (RowTypeInfo)typeInfo;
    System.out.println(rowTypeInfo);
    int tsIndex_a = rowTypeInfo.getFieldIndex("created_at");
    StringBuilder sb_a= new StringBuilder();
    for(String field:rowTypeInfo.getFieldNames()){
      if(field.equals("created_at")){
        sb_a.append(field+".rowtime,");
      }
      else {
        sb_a.append(field+",");
      }
    }
    String fields_a = sb_a.toString().substring(0,sb_a.length()-1);
    System.out.println("fields_a:"+fields_a);

    typeInfo = AvroSchemaConverter.convertToTypeInfo(avroSchema_b);
    rowTypeInfo = (RowTypeInfo)typeInfo;
    int tsIndex_b = rowTypeInfo.getFieldIndex("ts");
    StringBuilder sb_b= new StringBuilder();
    for(String field:rowTypeInfo.getFieldNames()){
      if(field.equals("ts")){
        sb_b.append(field+".rowtime,");
      }
      else {
        sb_b.append(field+",");
      }
    }
    String fields_b = sb_b.toString().substring(0,sb_b.length()-1);
    System.out.println("fields_b:"+fields_b);

    System.out.println("INDEX_A "+tsIndex_a);
    System.out.println("INDEX_B "+tsIndex_b);

    DataStream<Row> stream_a = env
            .addSource(new FlinkKafkaConsumer<>(topic_a, new MyAvroRowDeserializationSchema(avroSchema_a), properties))
            .assignTimestampsAndWatermarks(new RowExtractor(Time.of(10L, TimeUnit.SECONDS),tsIndex_a));
//            .windowAll(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(10)));
//    AllWindowedStream<Row, TimeWindow> stream_a = env
//            .addSource(new FlinkKafkaConsumer<>(topic_a, new MyAvroRowDeserializationSchema(avroSchema_a), properties))
//            .assignTimestampsAndWatermarks(new RowExtractor(Time.of(10L, TimeUnit.SECONDS),tsIndex_a))
//            .windowAll(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(10)));
//      Table tableA = tEnv.fromDataStream(stream_a);

    Table table_a = tEnv.fromDataStream(stream_a,fields_a);
    tEnv.registerTable("table_a", table_a);
//    tEnv.registerDataStream("table_a", stream_a);
//    Table result = tEnv.sqlQuery("select * from table_a");
////    System.out.println(result.getSchema().toRowDataType());
//    DataStream<Row> ds = tEnv.toAppendStream(result, TypeInformation.of(Row.class));
//
//    final Integer[] a = {0};
//    ds.addSink(new SinkFunction<Row>() {
//        @Override
//        public void invoke(Row value) throws Exception {
//          if (value != null)
//          System.out.println("STREAM_A:"+value.toString());
////          System.out.println("COUNT : "+ a[0]++);
//        }
//
//      });

    DataStream<Row> stream_b = env
            .addSource(new FlinkKafkaConsumer<>(topic_b, new MyAvroRowDeserializationSchema(avroSchema_b), properties))
            .assignTimestampsAndWatermarks(new RowExtractor(Time.of(10L, TimeUnit.SECONDS),tsIndex_b));

    Table table_b = tEnv.fromDataStream(stream_b,fields_b);
    tEnv.registerTable("table_b", table_b);
    Table result_c = tEnv.sqlQuery("select " +
            "table_a.event_id," +
            "table_b.event_id," +
            "table_a.created_at," +
            "cast (table_b.ts as timestamp) " +
            "from table_a,table_b" +
            " where table_a.event_id = table_b.event_id and table_a.created_at BETWEEN table_b.ts - INTERVAL '10' SECOND AND table_b.ts + INTERVAL '5' SECOND");

//    DataStream<Row> ds_b = tEnv.toAppendStream(result_b, TypeInformation.of(Row.class));
//    ds_b.addSink(new SinkFunction<Row>() {
//      @Override
//      public void invoke(Row value) throws Exception {
//        if (value != null)
//          System.out.println("STREAM_B:"+value.toString());
//      }
//    });

//    DataStream<Row> stream_simple = env
//            .addSource(new FlinkKafkaConsumer<>(topic_b, new MyAvroRowDeserializationSchema(avroSchema_c), properties));
//    stream_simple.print();

//    Schema avro_schema = new Schema.Parser().parse(avroSchema_c);
//    DataStreamSource<GenericRecord> stream = env.addSource(new FlinkKafkaConsumer<>(topic_b,AvroDeserializationSchema.forGeneric(avro_schema),properties));
//    stream.print();

    DataStream<Row> ds_c = tEnv.toAppendStream(result_c, TypeInformation.of(Row.class));
    ds_c.addSink(new SinkFunction<Row>() {
      @Override
      public void invoke(Row value) throws Exception {
        if (value != null)
          System.out.println("STREAM_C:"+value.toString());
      }
    });

      env.execute();

  }
  static class RowExtractor extends BoundedOutOfOrdernessTimestampExtractor<Row> {
    int timeStampIndex = 0;
    public RowExtractor(Time maxOutOfOrderness, int timeStampIndex) {
      super(maxOutOfOrderness);
      this.timeStampIndex = timeStampIndex;
    }
    @Override
    public long extractTimestamp(Row row) {
      return ((Timestamp) row.getField(timeStampIndex)).getTime();
    }
  }

  public static class SimpleDTO {
    public Integer event_id;

    public SimpleDTO(Integer event_id) {
      this.event_id = event_id;
    }
  }


}
