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

public class ReadFromKafka {


  public static void main(String[] args) throws Exception {
    // create execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
  //  EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

    StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(env, fsSettings);

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "data-nonprod-1:9092");
    properties.setProperty("group.id", "flink_consumer_3");
    properties.setProperty("zookeeper.connect", "data-nonprod-1:2181");

    String avroSchema = "{\"type\":\"record\",\"name\":\"PaymentCoreEventMessageDto\",\"namespace\":\"com.airwallex.pmtacpt.paycore.client.event\",\"fields\":[{\"name\":\"paymentAttemptResponseDto\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"PaymentAttemptResponseDto\",\"namespace\":\"com.airwallex.pmtacpt.paycore.client.dto.attempt\",\"fields\":[{\"name\":\"paymentAttemptId\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"paymentMethod\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"paymentInstrument\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"PaymentInstrumentResponseDto\",\"namespace\":\"com.airwallex.pmtacpt.paycore.client.dto.instrument\",\"fields\":[{\"name\":\"id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"requestId\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"customerId\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"paymentMethod\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"source\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Source\",\"fields\":[{\"name\":\"sourceType\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"card\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Card\",\"fields\":[{\"name\":\"cardNumber\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"panToken\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"cvcToken\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"expMonth\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"expYear\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"holderName\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"cvcCheck\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"country\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"fingerPrint\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"funding\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"last4\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"cardBin\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"brand\",\"type\":[\"null\",\"string\"],\"default\":null}]}],\"default\":null}]}],\"default\":null},{\"name\":\"billing\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Billing\",\"fields\":[{\"name\":\"customerId\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"customerPassword\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"dateOfBirth\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"personalId\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"phoneNumber\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"address\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Address\",\"namespace\":\"com.airwallex.pmtacpt.paycore.client.dto.common\",\"fields\":[{\"name\":\"country\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"state\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"city\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"street\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"postalCode\",\"type\":[\"null\",\"string\"],\"default\":null}]}],\"default\":null}]}],\"default\":null},{\"name\":\"liveMode\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"createdAt\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"updatedAt\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null}]}],\"default\":null},{\"name\":\"device\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Device\",\"namespace\":\"com.airwallex.pmtacpt.paycore.client.dto.transaction\",\"fields\":[{\"name\":\"deviceId\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"browserInfo\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ipAddress\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"hostName\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"cookiesAccepted\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"httpBrowserEmail\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"httpBrowserType\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ipNetworkAddress\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"browserDetail\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"BrowserDetail\",\"fields\":[{\"name\":\"userAgent\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"acceptHeader\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"language\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"colorDepth\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"screenHeight\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"screenWidth\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"timeZoneOffset\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"javaEnabled\",\"type\":[\"null\",\"boolean\"],\"default\":null}]}],\"default\":null}]}],\"default\":null},{\"name\":\"capturedAmount\",\"type\":[\"null\",{\"type\":\"string\",\"java-class\":\"java.math.BigDecimal\"}],\"default\":null},{\"name\":\"refundedAmount\",\"type\":[\"null\",{\"type\":\"string\",\"java-class\":\"java.math.BigDecimal\"}],\"default\":null},{\"name\":\"currency\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"status\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"createdAt\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"updatedAt\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null}]}],\"default\":null},{\"name\":\"paymentInstrumentResponseDto\",\"type\":[\"null\",\"com.airwallex.pmtacpt.paycore.client.dto.instrument.PaymentInstrumentResponseDto\"],\"default\":null},{\"name\":\"paymentIntentResponseDto\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"PaymentIntentResponseDto\",\"namespace\":\"com.airwallex.pmtacpt.paycore.client.dto.order\",\"fields\":[{\"name\":\"id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"requestId\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"customerId\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"totalAmount\",\"type\":[\"null\",{\"type\":\"string\",\"java-class\":\"java.math.BigDecimal\"}],\"default\":null},{\"name\":\"currency\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"order\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"PurchaseOrder\",\"fields\":[{\"name\":\"products\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"PhysicalProduct\",\"fields\":[{\"name\":\"type\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"code\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"sku\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"quantity\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"unitPrice\",\"type\":[\"null\",{\"type\":\"string\",\"java-class\":\"java.math.BigDecimal\"}],\"default\":null},{\"name\":\"desc\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"url\",\"type\":[\"null\",\"string\"],\"default\":null}]},\"java-class\":\"java.util.List\"}],\"default\":null},{\"name\":\"shippingData\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Shipping\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"phone\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"shippingMethod\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"address\",\"type\":[\"null\",\"com.airwallex.pmtacpt.paycore.client.dto.common.Address\"],\"default\":null}]}],\"default\":null},{\"name\":\"invoiceHeader\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"InvoiceHeader\",\"fields\":[{\"name\":\"giftIndicator\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"returnsAccepted\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"tenderType\",\"type\":[\"null\",\"string\"],\"default\":null}]}],\"default\":null},{\"name\":\"merchantOrderId\",\"type\":[\"null\",\"string\"],\"default\":null}]}],\"default\":null},{\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"phone\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"descriptor\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"liveMode\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"status\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"paymentAttempt\",\"type\":[\"null\",\"com.airwallex.pmtacpt.paycore.client.dto.attempt.PaymentAttemptResponseDto\"],\"default\":null},{\"name\":\"paymentMethods\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"PaymentMethod\",\"namespace\":\"com.airwallex.pmtacpt.paycore.client.dto.common\",\"fields\":[{\"name\":\"id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"type\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"schema\",\"type\":[\"null\",\"string\"],\"default\":null}]},\"java-class\":\"java.util.List\"}],\"default\":null},{\"name\":\"paymentInstruments\",\"type\":[\"null\",{\"type\":\"array\",\"items\":\"com.airwallex.pmtacpt.paycore.client.dto.instrument.PaymentInstrumentResponseDto\",\"java-class\":\"java.util.List\"}],\"default\":null},{\"name\":\"capturedAmount\",\"type\":[\"null\",{\"type\":\"string\",\"java-class\":\"java.math.BigDecimal\"}],\"default\":null},{\"name\":\"createdAt\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"updatedAt\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null}]}],\"default\":null},{\"name\":\"refundResponseDto\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"RefundResponseDto\",\"namespace\":\"com.airwallex.pmtacpt.paycore.client.dto.refund\",\"fields\":[{\"name\":\"id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"requestId\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"paymentAttemptId\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"refundAmount\",\"type\":[\"null\",{\"type\":\"string\",\"java-class\":\"java.math.BigDecimal\"}],\"default\":null},{\"name\":\"refundCurrency\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"reason\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"cancellationReason\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"status\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"createdAt\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"updatedAt\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"liveMode\",\"type\":[\"null\",\"boolean\"],\"default\":null}]}],\"default\":null},{\"name\":\"customerResponseDto\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"CustomerResponseDto\",\"namespace\":\"com.airwallex.pmtacpt.paycore.client.dto.customer\",\"fields\":[{\"name\":\"customerId\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"merchantRequestId\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"merchantCustomerId\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"address\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"customerType\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"phone\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"registerViaSocialMedia\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"firstSuccessfulOrderDate\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"registrationDate\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"additionalData\",\"type\":[\"null\",{\"type\":\"map\",\"values\":{\"type\":\"record\",\"name\":\"Object\",\"namespace\":\"java.lang\",\"fields\":[]}}],\"default\":null},{\"name\":\"liveMode\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"created\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null}]}],\"default\":null},{\"name\":\"eventId\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"eventType\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"merchantAccountId\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"merchantRequestId\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"paymentIntentId\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"paymentAttemptId\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"paymentDirectiveId\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"createdAt\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"error\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"liveMode\",\"type\":[\"null\",\"boolean\"],\"default\":null}]}";

  //  String mySchema = "{\"type\":\"record\",\"name\":\"MyPaym" +
     //       "entCoreInstrumentEventMessageDto4\",\"namespace\":\"com.airwallex.kotlin.kafka\",\"fields\":[{\"name\":\"type\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"num\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"time\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null}]}";
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<>("c1.auqirer_core_event", new MyAvroRowDeserializationSchema(avroSchema), properties);
    consumer.setStartFromEarliest();

    DataStream<Row> stream = env.addSource(consumer);

    TypeInformation<Row> typeInfo = AvroSchemaConverter.convertToTypeInfo(avroSchema);
    RowTypeInfo rowTypeInfo = (RowTypeInfo) typeInfo;

    stream = stream.assignTimestampsAndWatermarks(new RowExtractor(Time.of(10L, TimeUnit.SECONDS), rowTypeInfo.getFieldIndex("createdAt")));

    String fields = convertToRowtimeFlied(rowTypeInfo.getFieldNames(), "createdAt");

    Table table1 = fsTableEnv.fromDataStream(stream, fields);
    //Table table1 = fsTableEnv.fromDataStream(stream);
//
//      String sqlString = "SELECT paymentAttemptResponseDto.paymentAttemptId," +
//              " paymentAttemptResponseDto.paymentMethod, paymentAttemptResponseDto.status," +
//              " paymentAttemptResponseDto.createdAt, paymentAttemptResponseDto.updatedAt FROM ";

    // String sqlString = "SELECT paymentAttemptResponseDto.status, paymentAttemptResponseDto.createdAt, mytime FROM ";
   //  String sqlString = "SELECT TUMBLE_START(createdAt, INTERVAL '1' MINUTE) as rowTime, paymentAttemptResponseDto.status , count(*) as c FROM ";
    String sqlString = "SELECT count(*) as c, paymentIntentResponseDto.status FROM ";

    table1.printSchema();
  // String sqlString = "SELECT   count(*) as number, type FROM ";
   //    String sqlString = "SELECT paymentIntentResponseDto.status FROM ";

       String condition = " group by TUMBLE(createdAt, INTERVAL '1' MINUTE), paymentIntentResponseDto.status";
   //   String condition = "  GROUP BY HOP(createAt, INTERVAL '1' MINUTE, INTERVAL '5' DAY), paymentIntentResponseDto.status.status";

  //  String condition = " WHERE num is not null ";
 //   String condition = "  group by TUMBLE(" +  "`time`, INTERVAL '3' SECOND),type";

//     String condition = " WHERE paymentIntentResponseDto is not null ";

      Table result = fsTableEnv.sqlQuery(sqlString + table1 + condition);

 //   System.out.println(table1.getSchema().toRowDataType().toString());

    fsTableEnv.toAppendStream(result, Row.class).print();

    //   fsTableEnv.toRetractStream(result, Row.class).print();
  //.print();

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
    //  if(row.getField(timeStampIndex) == null){ return new Timestamp(-1).getTime();}
     // return  (long)row.getField(timeStampIndex);
      return ((Timestamp) row.getField(timeStampIndex)).getTime();
    }
  }

  private static String convertToRowtimeFlied(String[] oldFields, String rowTimeFieldName){

    StringBuilder sb= new StringBuilder();

    for(String field: oldFields){
      if(field.equals(rowTimeFieldName)){
        sb.append(field+".rowtime,");
      }
      else {
        sb.append(field+",");
      }
    }

    String fields = sb.toString().substring(0, sb.length() - 1);
    return fields;
  }

}


