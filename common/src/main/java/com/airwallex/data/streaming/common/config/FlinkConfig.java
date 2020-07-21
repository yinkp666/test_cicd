package com.airwallex.data.streaming.common.config;


import org.apache.flink.streaming.api.windowing.time.Time;
import org.ho.yaml.Yaml;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class FlinkConfig {


    private FlinkConfig(String name, String minIdleTime, String maxIdleTime, List<QueryConfig> queryConfigs,
                        List<KafkaSourceConfig> kafkaSourceConfigs, Map<String, SinkInstance> sinkInstanceMap, Map<String, Object> checkPointConfig) {
        this.name = name;
        this.minIdleTime = minIdleTime;
        this.maxIdleTime = maxIdleTime;
        this.queryConfigs = queryConfigs;
        this.kafkaSourceConfigs = kafkaSourceConfigs;
        this.sinkInstanceMap = sinkInstanceMap;
        this.checkPointConfig = checkPointConfig;
    }

    public String getName() {
        return name;
    }

    String name;
    String minIdleTime;
    String maxIdleTime;
    private final Map<String, Object> checkPointConfig;

    public Map<String, Object> getCheckPointConfig() {
        return checkPointConfig;
    }

    public String getMinIdleTime() {
        return minIdleTime;
    }

    public String getMaxIdleTime() {
        return maxIdleTime;
    }

    public List<QueryConfig> getQueryConfigs() {
        return queryConfigs;
    }

    public List<KafkaSourceConfig> getKafkaSourceConfigs() {
        return kafkaSourceConfigs;
    }

    public Map<String, SinkInstance> getSinkInstanceMap() {
        return sinkInstanceMap;
    }

    List<QueryConfig> queryConfigs;
    List<KafkaSourceConfig> kafkaSourceConfigs;
    Map<String, SinkInstance> sinkInstanceMap;

    public static FlinkConfig loadFlinkConfig(String yamlConfigFile) throws FileNotFoundException {
        File dumpFile = new File(yamlConfigFile);
        Map<String, Object> configs = Yaml.loadType(dumpFile, HashMap.class);
        String configName = (String) configs.getOrDefault("name", "flink-streaming");
        String minIdleTime = (String) configs.getOrDefault("minIdleTime", "1");
        String maxIdleTime = (String) configs.getOrDefault("maxIdleTime", "1");
        return new FlinkConfig(configName,minIdleTime,maxIdleTime, getSqlPlanConfigs(configs), getKafkaSourceConfigs(configs), getOutputConfigs(configs),getCheckPointConfig(configs));
    }

    private static Map<String, Object> getCheckPointConfig(Map<String, Object> configs) {
        return (Map<String, Object>) configs.get("checkpoint");
    }

    private static List<KafkaSourceConfig> getKafkaSourceConfigs(Map<String, Object> configs) {
        List<Map<String, Object>> configProps = (List<Map<String, Object>>) configs.get("kafkaSources");
        List<KafkaSourceConfig> kafkaSourceConfigs = new ArrayList<>();

        for (Map<String, Object> prop : configProps) {
            kafkaSourceConfigs.add(new KafkaSourceConfig(prop));
        }
        return kafkaSourceConfigs;
    }

    private static Map<String, SinkInstance> getOutputConfigs(Map<String, Object> configs) {
        Map<String, SinkInstance> outputConfigs = new HashMap<>();
        List<Map<String, Object>> configProps = (List<Map<String, Object>>) configs.get("sinkInstances");

        for (Map<String, Object> prop : configProps) {
            SinkInstance sinkInstance = new SinkInstance(prop);
            outputConfigs.put(sinkInstance.getInstance(), sinkInstance);
        }
        return outputConfigs;
    }

    private static List<QueryConfig> getSqlPlanConfigs(Map<String, Object> configs) {

        List<QueryConfig> queryConfigs = new ArrayList<>();

        List<Map<String, Object>> configProps = (List<Map<String, Object>>) configs.get("queryPlans");
        for (Map<String, Object> prop : configProps) {
            QueryConfig queryConfig = new QueryConfig(prop);
            queryConfigs.add(queryConfig);
        }
        return queryConfigs;
    }


    public static void main(String[] args) throws FileNotFoundException {

        FlinkConfig flinkConfig = FlinkConfig.loadFlinkConfig("conf/config.yaml");
        System.out.println(flinkConfig);
    }

    @Override
    public String toString() {
        return "FlinkConfig{" +
                "\nqueryConfigs=" + queryConfigs +
                ",\nkafkaSourceConfigs=" + kafkaSourceConfigs +
                ", \nsinkInstanceMap=" + sinkInstanceMap +
                "\n}";
    }


    public static class KafkaSourceConfig {
        public Properties getProperties() {
            return properties;
        }

        public String getTopic() {
            return topic;
        }


        public String getRowtime() {
            return rowtime;
        }

        public Time getMaxOutOfOrderness() {
            return maxOutOfOrderness;
        }

        public Properties properties;
        public String topic;
        public String rowtime;
        public Time maxOutOfOrderness;
        public Temporal temporal;
        public Boolean isKeyedStream;
        public String keyField;
        public Long slideWindowSize;

        public Temporal getTemporal() {
            return temporal;
        }

        public long getSlideWindowSize() {
            return slideWindowSize;
        }

        public Boolean getKeyedStream() {
            return isKeyedStream;
        }

        public String getKeyField() {
            return keyField;
        }

        public FormatConfig getFormatConfig() {
            return formatConfig;
        }

        public void setFormatConfig(FormatConfig formatConfig) {
            this.formatConfig = formatConfig;
        }

        FormatConfig formatConfig;
        public String getTable() {
            return table;
        }

        String table;

        public KafkaSourceConfig(Map<String, Object> source) {

            this.properties = initProps((Map<String, String>) source.get("kafka"));
            this.topic = (String) source.get("topic");
            this.formatConfig = new FormatConfig((Map<String, Object>) source.get("format"));
            this.rowtime = (String) source.get("rowtime");
            if (rowtime != null)
                this.maxOutOfOrderness = initMaxOutOfOrderness(source);
            this.table = (String) source.get("table");

            if(source.containsKey("temporal")){
                temporal = new Temporal((Map<String, String>) source.get("temporal"));
            }
            this.isKeyedStream = (Boolean) source.get("isKeyedStream");
            this.keyField = (String) source.get("keyField");
            this.slideWindowSize = source.get("slideWindowSize") == null? -1 : Long.parseLong(source.get("slideWindowSize").toString());
        }

        private Time initMaxOutOfOrderness(Map<String, Object> source) {
            Map<String, Object> watermark = (Map<String, Object>) source.get("watermark");

            return Time.of((int) watermark.get("maxlatency"), TimeUnit.valueOf((String) watermark.get("timeUnit")));
        }

        @Override
        public String toString() {
            return "KafkaSourceConfig{" +
                    "properties=" + properties +
                    ", topic='" + topic + '\'' +
                    ", rowtime='" + rowtime + '\'' +
                    ", maxOutOfOrderness=" + maxOutOfOrderness +
                    ", formatConfig=" + formatConfig +
                    ", table='" + table + '\'' +
                    '}';
        }
    }

    public static  class Temporal{
        private Temporal(String function, String primaryKey) {
            this.function = function;
            this.primaryKey = primaryKey;
        }

        public Temporal(Map<String, String> temporal) {
            this(temporal.get("function"),temporal.get("primaryKey"));
        }

        public String getFunction() {
            return function;
        }

        public void setFunction(String function) {
            this.function = function;
        }

        public String getPrimaryKey() {
            return primaryKey;
        }

        public void setPrimaryKey(String primaryKey) {
            this.primaryKey = primaryKey;
        }

        public String function;
        public String primaryKey;

    }
    public static  class FormatConfig{

        public FormatConfig(Map<String, Object> formatProp) {
            this.type = (String) formatProp.get("type");
            this.properties = (Map<String, String>) formatProp.get("properties");
        }

        String type;

        public String getType() {
            return type;
        }

        public Map<String, String> getProperties() {
            return properties;
        }

        public Map<String, String> properties;

        @Override
        public String toString() {
            return "FormatConfig{" +
                    "type='" + type + '\'' +
                    ", properties=" + properties +
                    '}';
        }
    }

    public static class SinkInstance {
        public String getInstance() {
            return instance;
        }

        public String getType() {
            return type;
        }

        public Properties getProps() {
            return props;
        }

        String instance;
        String type;
        Properties props;

        public SinkInstance(Map<String, Object> outputProp) {
            this.instance = (String) outputProp.get("instance");
            this.type = (String) outputProp.get("type");
            props = initProps((Map<String, String>) outputProp.get("props"));
        }


        @Override
        public String toString() {
            return "SinkInstance{" +
                    "instance='" + instance + '\'' +
                    ", type='" + type + '\'' +
                    ", props=" + props +
                    '}';
        }
    }

    public static class QueryConfig {
        public String getQuery() {
            return query;
        }

        public String getSinkInstance() {
            return sinkInstance;
        }

        public Properties getProps() {
            return props;
        }

        String query;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        String name;
        String sinkInstance;
        Properties props;

        public String getSinkMode() {
            return sinkMode;
        }

        String sinkMode;

        public QueryConfig(Map<String, Object> configs) {
            this.query = (String) configs.get("query");
            Map<String, Object> sink = (Map<String, Object>) configs.get("sink");
            this.sinkInstance = (String) sink.get("instance");
            this.sinkMode = (String) sink.get("mode");
            props = initProps((Map<String, String>) sink.get("props"));
            this.name = (String) configs.get("name");

        }


        @Override
        public String toString() {
            return "QueryConfig{" +
                    "query='" + query + '\'' +
                    ", name='" + name + '\'' +
                    ", sinkInstance='" + sinkInstance + '\'' +
                    ", props=" + props +
                    '}';
        }
    }

    private static Properties initProps(Map<String, String> propMap) {
        Properties properties = new Properties();
        if (propMap != null) {
            for (Map.Entry<String, String> entry : propMap.entrySet()) {
                properties.put(entry.getKey(), entry.getValue());
            }
        }
        return properties;
    }
}
