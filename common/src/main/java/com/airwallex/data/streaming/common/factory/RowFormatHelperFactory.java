package com.airwallex.data.streaming.common.factory;

import com.airwallex.data.streaming.common.config.FlinkConfig;
import com.airwallex.data.streaming.common.rowformat.AvroRowFormatHelper;
import com.airwallex.data.streaming.common.rowformat.CSVRowFormatHelper;
import com.airwallex.data.streaming.common.rowformat.RowFormatHelper;

import java.util.Map;

public class RowFormatHelperFactory {

    public static RowFormatHelper createFormatHelper(FlinkConfig.FormatConfig formatConfig) {
        Map<String, String> props = formatConfig.getProperties();
        switch (formatConfig.getType()) {
            case "csv":
                return new CSVRowFormatHelper(props);
            case "avro":
                return new AvroRowFormatHelper(props);
        }
        return null;

    }
}
