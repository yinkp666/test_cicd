package com.airwallex.data.streaming.common.rowformat;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.types.Row;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.json.simple.JSONObject;

import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

public class AvroRowFormatHelper extends RowFormatHelper {

    public AvroRowFormatHelper(Map<String, String> properties) {
        super(properties);
    }

    @Override
    protected RowTypeInfo buildRowTypeInfo(Map<String, String> properties) {

        //   null value (disabled by default)));
//        String pureSchema = (String) properties.get("avroSchema");
        String pureSchema = null;
        if(properties.get("avroSchemaFile") != null){
            JSONParser parser = new JSONParser();
            try {
                Object obj = parser.parse(new FileReader(properties.get("avroSchemaFile")));
                JSONObject jsonNode = (JSONObject)obj;
                pureSchema = jsonNode.toJSONString();
            } catch (IOException | ParseException e) {
                e.printStackTrace();
            }
        }
        else {
            pureSchema = (String) properties.get("avroSchema");
        }
        String avroSchema = cleanSchema(pureSchema);

        TypeInformation<Row> r = AvroSchemaConverter.convertToTypeInfo(avroSchema);
        return  (RowTypeInfo) r;

    }

    @Override
    protected DeserializationSchema<Row> buildDeserializationSchema(RowTypeInfo rowTypeInfo, Map<String, String> properties) {
        String pureSchema = null;
        if(properties.get("avroSchemaFile") != null){
            JSONParser parser = new JSONParser();
            try {
                Object obj = parser.parse(new FileReader(properties.get("avroSchemaFile")));
                JSONObject jsonNode = (JSONObject)obj;
                pureSchema = jsonNode.toJSONString();
            } catch (IOException | ParseException e) {
                e.printStackTrace();
            }
        }
        else {
            pureSchema = (String) properties.get("avroSchema");
        }
        assert pureSchema != null;
        String avroSchema = cleanSchema(pureSchema);

        return  new MyAvroRowDeserializationSchema(avroSchema);
    }

    private static String cleanSchema(String pureSchema) {
        return pureSchema.replaceAll("\\{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}", "\"long\"");

    }
}
