package com.airwallex.data.streaming.common.dimension;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;

public class JoinWithBigtable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        //  EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(env, fsSettings);


        List<Tuple2<String,String>> lists= Arrays.asList(Tuple2.of("139012321323","ca"));

        DataStream<Tuple2<String,String>> events  = env.fromCollection(lists);

        Table orgin = fsTableEnv.fromDataStream(events, "phone,name,proctime.proctime");

        fsTableEnv.registerTable("orin",orgin);

//        Table result = fsTableEnv.sqlQuery(sqlString + table1 + condition);

        //   System.out.println(table1.getSchema().toRowDataType().toString());

//        fsTableEnv.toAppendStream(result, Row.class).print();

        TypeInformation[] types = new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING};
        String[] fields = new String[]{"phone", "mid", "mail"};

        DemoLookupableTableSource demoLookupableTableSource = new DemoLookupableTableSource(fields, types, false);

        fsTableEnv.registerTableSource("info",demoLookupableTableSource);
        String sql = "select a.*,i.*" +
                " from" +
                " orin as a" +
                " join info FOR SYSTEM_TIME AS OF a.proctime as i on i.phone = a.phone";

        Table result = fsTableEnv.sqlQuery(sql);

        //   System.out.println(table1.getSchema().toRowDataType().toString());

        fsTableEnv.toAppendStream(result, Row.class).print();

        env.execute();
    }
}
