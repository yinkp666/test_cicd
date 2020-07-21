package com.airwallex.data.streaming.common.UDFS;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class UDFTestMain {
    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();

        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        testScalarFunction(bsEnv, bsTableEnv);
        testTableFunction(bsEnv, bsTableEnv);
        testAggregationFunction(bsEnv, bsTableEnv);
        testTableAggregationFunction(bsEnv, bsTableEnv);

        bsEnv.execute("udf test....");
    }

    private static void testTableFunction(StreamExecutionEnvironment bsEnv, StreamTableEnvironment bsTableEnv) {

        // Create a DataStream from a list of elements
        DataStream<String> myInts = bsEnv.fromElements("1, 2, 3, 4, 5", "hi,hello");

        Configuration conf = new Configuration();
        conf.setString("separator", ",");
        bsEnv.getConfig().setGlobalJobParameters(conf);

// register the function
        bsTableEnv.registerFunction("split", new Split());
        Table myTable = bsTableEnv.fromDataStream(myInts, "a");

// Use the table function in the Java Table API. "as" specifies the field names of the table.
        myTable.joinLateral("split(a) as (word, length)")
                .select("a, word, length");
        Table t1 = myTable.leftOuterJoinLateral("split(a) as (word, length)")
                .select("a, word, length");
        bsTableEnv.registerTable("mmm", myTable);
// Use the table function in SQL with LATERAL and TABLE keywords.
// CROSS JOIN a table function (equivalent to "join" in Table API).
        bsTableEnv.sqlQuery("SELECT a, t.* FROM mmm, LATERAL TABLE(split(mmm.a)) as t");
//        printTable(t1, bsTableEnv);

        bsTableEnv.registerFunction("csplit", new Split.CustomTypeSplit());
// LEFT JOIN a table function (equivalent to "leftOuterJoin" in Table API).
        Table t = bsTableEnv.sqlQuery("SELECT a, word, length FROM mmm LEFT JOIN LATERAL TABLE(csplit(a)) as T(word, length) ON TRUE");
        printTable2(t, bsTableEnv);
    }

    private static void testAggregationFunction(StreamExecutionEnvironment bsEnv, StreamTableEnvironment bsTableEnv) {

        DataStream<Tuple3> myInts = bsEnv.fromElements(Tuple3.of("user1", 1L, 2), Tuple3.of("user1", 3L, 2));

        bsTableEnv.registerFunction("wAvg", new WeightedAvg());

        Table myTable = bsTableEnv.fromDataStream(myInts, "user,points,level");

        bsTableEnv.registerTable("userScores", myTable);
        Table t = bsTableEnv.sqlQuery("SELECT user, wAvg(points, level) AS avgPoints FROM userScores GROUP BY user");
// register the function
        printTable(t, bsTableEnv);
    }

    private static void testTableAggregationFunction(StreamExecutionEnvironment bsEnv, StreamTableEnvironment bsTableEnv) {
        // create execution environment
        // Create a DataStream from a list of elements
        DataStream<Tuple2> myInts = bsEnv.fromElements(
                Tuple2.of("key", 1),
                Tuple2.of("key", 2),
                Tuple2.of("key", 8),
                Tuple2.of("key", 0));

        bsTableEnv.registerFunction("top2", new Top2());

        Table myTable = bsTableEnv.fromDataStream(myInts, "key,a");

        Table t = myTable.groupBy("key").flatAggregate("top2(a) as (v, rank)")
                .select("v, rank");

        printTable(t, bsTableEnv);
    }

    private static void testScalarFunction(StreamExecutionEnvironment bsEnv, StreamTableEnvironment bsTableEnv) {

        // Create a DataStream from a list of elements
        DataStream<String> myInts = bsEnv.fromElements("1, 2, 3, 4, 5");

// set job parameter
        Configuration conf = new Configuration();
        conf.setString("hashcode_factor", "2");
        bsEnv.getConfig().setGlobalJobParameters(conf);

// register the function
        bsTableEnv.registerFunction("hashCode", new HashCode());
        Table myTable = bsTableEnv.fromDataStream(myInts, "sss");
// use the function in Java Table API
        bsTableEnv.toAppendStream(myTable.select("sss, sss.hashCode(), hashCode(sss)"), Row.class).print();

        bsTableEnv.registerTable("ttt", myTable);
// use the function in SQL
        bsTableEnv.toAppendStream(bsTableEnv.sqlQuery("SELECT sss, HASHCODE(sss) FROM ttt"), Row.class).print();

    }

    private static void printTable(Table table, StreamTableEnvironment bsTableEnv) {
        System.out.println("===========");
        bsTableEnv.toRetractStream(table, Row.class).print();

    }

    private static void printTable2(Table table, StreamTableEnvironment bsTableEnv) {
        System.out.println("===========");
        bsTableEnv.toAppendStream(table, Row.class).print();

    }
}
