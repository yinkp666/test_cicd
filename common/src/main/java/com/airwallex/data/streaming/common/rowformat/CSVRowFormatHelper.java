package com.airwallex.data.streaming.common.rowformat;

import com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.csv.CsvRowDeserializationSchema;
import org.apache.flink.types.Row;

import java.util.Map;

public class CSVRowFormatHelper extends RowFormatHelper {

    public CSVRowFormatHelper(Map<String, String> properties) {
        super(properties);
    }

    @Override
    protected RowTypeInfo buildRowTypeInfo(Map<String, String> properties) {
        String[] fields = properties.get("fields").split(",");

        String[] types = properties.get("types").split(",");
        Map<String, TypeInformation> supportedFieldTypes = ImmutableMap.of("LONG", Types.LONG, "STRING", Types.STRING, "INT", Types.INT, "SQL_TIMESTAMP", Types.SQL_TIMESTAMP);

        TypeInformation rowTypeInfo[] = new TypeInformation[types.length];
        for (int i = 0; i < fields.length; i++) {
            rowTypeInfo[i] = supportedFieldTypes.get(types[i]);
        }
        return new RowTypeInfo(rowTypeInfo, fields);

    }

    @Override
    protected DeserializationSchema<Row> buildDeserializationSchema(RowTypeInfo rowTypeInfo, Map<String, String> properties) {
        CsvRowDeserializationSchema.Builder builder = new CsvRowDeserializationSchema.Builder(rowTypeInfo);
        builder.setIgnoreParseErrors(true);
        builder.setFieldDelimiter(properties.get("delimiter").charAt(0));

        return builder.build();
    }
}
