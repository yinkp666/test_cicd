package com.airwallex.data.streaming.common.template;

import com.airwallex.data.streaming.common.config.FlinkConfig;
import com.airwallex.data.streaming.common.factory.TableSinkFactory;
import com.airwallex.data.streaming.common.function.CountLongPeriodFunction;
import com.airwallex.data.streaming.common.item.SimpleCountItem;
import com.airwallex.data.streaming.common.rowformat.RowFormatHelper;
import com.airwallex.data.streaming.common.factory.RowFormatHelperFactory;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class SimpleCountStream {

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

        /**
         * init checkpoint config for failure tolerance
         */
        initCheckpointConfig(bsEnv, flinkConfig.getCheckPointConfig());


        int minIdleTime = Integer.parseInt(flinkConfig.getMinIdleTime());
        int maxIdleTime = Integer.parseInt(flinkConfig.getMaxIdleTime());

        if(minIdleTime != maxIdleTime && maxIdleTime != 1){
            bsTableEnv.getConfig().setIdleStateRetentionTime(org.apache.flink.api.common.time.Time.minutes(minIdleTime),org.apache.flink.api.common.time.Time.minutes(maxIdleTime));
        }

        List<FlinkConfig.KafkaSourceConfig> sources = flinkConfig.getKafkaSourceConfigs();
        Map<String, FlinkConfig.SinkInstance> sinkInstanceMap = flinkConfig.getSinkInstanceMap();
        for (FlinkConfig.KafkaSourceConfig source : sources) {

            DataStream<Row> stream = registerSource(bsTableEnv, bsEnv, source);

            for (FlinkConfig.QueryConfig queryConfig : flinkConfig.getQueryConfigs()) {

//            Table result = bsTableEnv.sqlQuery(queryConfig.getQuery());
//            bsTableEnv.registerTable(queryConfig.getName(), result);
//            String sinkMode = queryConfig.getSinkMode();
            stream.addSink(TableSinkFactory.createAppendSink(queryConfig.getProps(), sinkInstanceMap.get(queryConfig.getSinkInstance())));

            }

        }

//        Map<String, FlinkConfig.SinkInstance> sinkInstanceMap = flinkConfig.getSinkInstanceMap();

//        for (FlinkConfig.QueryConfig queryConfig : flinkConfig.getQueryConfigs()) {
//
//            Table result = bsTableEnv.sqlQuery(queryConfig.getQuery());
//            bsTableEnv.registerTable(queryConfig.getName(), result);
//            String sinkMode = queryConfig.getSinkMode();
//
//            if (sinkMode.equals("append")) {
//                SinkFunction<Row> tableSink = TableSinkFactory.createAppendSink(queryConfig.getProps(), sinkInstanceMap.get(queryConfig.getSinkInstance()));
//                DataStream<Row> dataStream = bsTableEnv.toAppendStream(result, Row.class);
//                dataStream.addSink(tableSink);
//            } else {
//                SinkFunction<Tuple2<Boolean, Row>> tableSink = TableSinkFactory.createRetractSink(queryConfig.getProps(), sinkInstanceMap.get(queryConfig.getSinkInstance()));
//
//                DataStream<Tuple2<Boolean, Row>> dataStream = bsTableEnv.toRetractStream(result, Row.class);
//
//                dataStream.addSink(tableSink);
//            }
//        }

        bsEnv.execute(flinkConfig.getName());
    }

    private static DataStream<Row> registerSource(StreamTableEnvironment bsTableEnv, StreamExecutionEnvironment bsEnv, FlinkConfig.KafkaSourceConfig kafkaSourceConfig){
        FlinkConfig.FormatConfig formatConfig = kafkaSourceConfig.getFormatConfig();

        RowFormatHelper rowFormatHelper = RowFormatHelperFactory.createFormatHelper(formatConfig);

        DeserializationSchema<Row> valueDeserializer = rowFormatHelper.getValueDeserializer();

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
        if(null != isKeyedStream && isKeyedStream){
            Table table1 = bsTableEnv.fromDataStream(stream);
            Table result = bsTableEnv.sqlQuery("SELECT " +keyField+ ","+ rowTime + " FROM " + table1);
            DataStream<Row> st = bsTableEnv.toAppendStream(result, Row.class)
                    .map((MapFunction<Row, SimpleCountItem>) value -> {
                        String key = String.valueOf(value.getField(0));
                        long rowTime1 = (long) value.getField(1);
                        return new SimpleCountItem(key, rowTime1);
                    }).keyBy("key").process(new CountLongPeriodFunction(slideWindowSize)).forward();

            return st;

        }else return null;


    }

    private static void initCheckpointConfig(StreamExecutionEnvironment env, Map<String, Object> checkPointConfig) throws IOException {

        Boolean enableCheckPoint = (Boolean) checkPointConfig.get("enable.checkpoint");
        String stateBackend = (String) checkPointConfig.get("state.backend");

        if (enableCheckPoint == true && stateBackend != "") {
//            logger.info("enable checkpoint:" + checkPointConfig);
            CheckpointConfig checkpointConfig = env.getCheckpointConfig();
            checkpointConfig.setFailOnCheckpointingErrors(false);
            checkpointConfig.setCheckpointInterval(10000);
            checkpointConfig.setMinPauseBetweenCheckpoints(5000);
            checkpointConfig.setMaxConcurrentCheckpoints(1);
            checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
            checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(
                    stateBackend,
                    true);
            env.setStateBackend((StateBackend) rocksDBStateBackend);
        } else {
            System.out.println(("disable checkpoint:" + checkPointConfig));
        }

    }
}
