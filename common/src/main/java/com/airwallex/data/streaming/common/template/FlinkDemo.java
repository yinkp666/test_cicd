package com.airwallex.data.streaming.common.template;

import com.airwallex.data.streaming.common.config.FlinkConfig;
import com.airwallex.data.streaming.common.extractor.RowExtractor;
import com.airwallex.data.streaming.common.factory.RowFormatHelperFactory;
import com.airwallex.data.streaming.common.factory.TableSinkFactory;
import com.airwallex.data.streaming.common.rowformat.RowFormatHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class FlinkDemo {

    private final static String EVENT_TIME = "ti";

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Please specific the  yaml config file as first argument.");
            System.exit(1);
        }
        String configFile = args[0];
        FlinkConfig flinkConfig = FlinkConfig.loadFlinkConfig(configFile);

        // create execution environment
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<FlinkConfig.KafkaSourceConfig> sources = flinkConfig.getKafkaSourceConfigs();
        for (FlinkConfig.KafkaSourceConfig source : sources) {

            registerSourceTable(bsTableEnv, bsEnv, source);


        }

        Map<String, FlinkConfig.SinkInstance> sinkInstanceMap = flinkConfig.getSinkInstanceMap();

        for (FlinkConfig.QueryConfig queryConfig : flinkConfig.getQueryConfigs()) {

            Table result = bsTableEnv.sqlQuery(queryConfig.getQuery());
            bsTableEnv.registerTable(queryConfig.getName(), result);
            String sinkMode = queryConfig.getSinkMode();

            if (sinkMode.equals("append")) {
                SinkFunction<Row> tableSink = TableSinkFactory.createAppendSink(queryConfig.getProps(), sinkInstanceMap.get(queryConfig.getSinkInstance()));
                DataStream<Row> dataStream = bsTableEnv.toAppendStream(result, Row.class);
                dataStream.addSink(tableSink);
            } else {
                SinkFunction<Tuple2<Boolean, Row>> tableSink = TableSinkFactory.createRetractSink(queryConfig.getProps(), sinkInstanceMap.get(queryConfig.getSinkInstance()));

                DataStream<Tuple2<Boolean, Row>> dataStream = bsTableEnv.toRetractStream(result, Row.class);

                dataStream.addSink(tableSink);
            }
        }

        bsEnv.execute(flinkConfig.getName());
    }

    private static void registerTemporalFunction(StreamTableEnvironment bsTableEnv, FlinkConfig.KafkaSourceConfig sourceConfig) {
        Table tab = bsTableEnv.scan(sourceConfig.getTable());

        TemporalTableFunction temporalTableFunction = tab.createTemporalTableFunction(EVENT_TIME, sourceConfig.temporal.primaryKey);

        bsTableEnv.registerFunction(sourceConfig.temporal.function, temporalTableFunction);

    }

    private static void registerSourceTableWithCsv(StreamTableEnvironment bsTableEnv, FlinkConfig.KafkaSourceConfig source) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "data-nonprod-1:9092");
        properties.setProperty("zookeeper.connect", "data-nonprod-1:2181");
        properties.setProperty("group.id", "flink_consumer_3");
        Map<String, String> formatProps = source.getFormatConfig().properties;
//        TableSource t= TableFactoryUtil.findAndCreateTableSource( new Kafka().properties(properties).topic("foo").startFromEarliest().version("universal"));

        Schema schema = new Schema();
        String[] fields = formatProps.get("fields").split(",");

        String[] types = formatProps.get("types").split(",");

        Csv csv = new Csv()
                .deriveSchema()
                .fieldDelimiter(formatProps.get("delimiter").charAt(0))
                .ignoreParseErrors();
        String rowTime = source.getRowtime();
        for (int i = 0; i < fields.length; i++) {
            if (fields[i].equalsIgnoreCase(rowTime)) {
                schema.field(EVENT_TIME, types[i]);
                long delay = source.getMaxOutOfOrderness().toMilliseconds();
                schema.rowtime(new Rowtime().timestampsFromField(rowTime).watermarksPeriodicBounded(delay));
            } else {
                schema.field(fields[i], types[i]);
            }
        }

        bsTableEnv.connect(
                new Kafka()
                        .properties(source.getProperties())
                        .topic(source.getTopic())
                        .version("universal"))
                .withFormat(csv).inAppendMode()
                .withSchema(schema).createTemporaryTable(source.getTable());
                // register as source, sink, or both and under a name

        bsTableEnv.scan(source.getTable()).printSchema();
    }

    private static void registerSourceTable(StreamTableEnvironment bsTableEnv, StreamExecutionEnvironment bsEnv, FlinkConfig.KafkaSourceConfig kafkaSourceConfig) {
        //   null value (disabled by default)));
        FlinkConfig.FormatConfig formatConfig = kafkaSourceConfig.getFormatConfig();

        RowFormatHelper rowFormatHelper = RowFormatHelperFactory.createFormatHelper(formatConfig);

        RowTypeInfo rowTypeInfos = rowFormatHelper.getRowTypeInfo();

        DeserializationSchema<Row> valueDeserializer = rowFormatHelper.getValueDeserializer();

        TypeInformation<Row> typeInfoWithTime = getTypeInfoWithTime(rowTypeInfos);

        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<>(
                kafkaSourceConfig.getTopic(),
                valueDeserializer,
                kafkaSourceConfig.getProperties()
        );

        String rowTime = kafkaSourceConfig.getRowtime();

        Table table;
        if (rowTime != null) {
            DataStream<Row> stream = bsEnv.addSource(consumer);
            Table table1 = bsTableEnv.fromDataStream(stream);

            // Add ti as top-level timestamp
            Table result = bsTableEnv.sqlQuery("SELECT *,  " + rowTime + " as " + EVENT_TIME + " FROM " + table1);

            DataStream<Row> st = bsTableEnv.toAppendStream(result, Row.class);

            int fieldIndex = typeInfoWithTime.getArity() - 1;
            RowExtractor rowExtractor = new RowExtractor(kafkaSourceConfig.getMaxOutOfOrderness(), fieldIndex, Types.LONG);

            DataStream<Row> st2 = st.map((MapFunction<Row, Row>) row -> row).returns(typeInfoWithTime).assignTimestampsAndWatermarks(rowExtractor);

            String fields = StringUtils.join(((RowTypeInfo) typeInfoWithTime).getFieldNames(), ",") + ".rowtime";

            table = bsTableEnv.fromDataStream(st2, fields);
        } else {
            table = bsTableEnv.fromDataStream(bsEnv.addSource(consumer));
        }

        bsTableEnv.registerTable(kafkaSourceConfig.getTable(), table);

        table.printSchema();

        /**
         * register temporal function
         */
//        if (kafkaSourceConfig.temporal != null) {
//            registerTemporalFunction(bsTableEnv, kafkaSourceConfig);
//        }
    }

    private static TypeInformation<Row> getTypeInfoWithTime(RowTypeInfo rowTypeInfo) {

        int newLength = rowTypeInfo.getArity() + 1;
        TypeInformation[] rowTypeInfos = Arrays.copyOf(rowTypeInfo.getFieldTypes(), newLength);

        rowTypeInfos[newLength - 1] = Types.LONG;
        String[] fieldsArray = Arrays.copyOf(rowTypeInfo.getFieldNames(), newLength);
        fieldsArray[newLength - 1] = EVENT_TIME;
        return new RowTypeInfo(rowTypeInfos, fieldsArray);
    }

}
