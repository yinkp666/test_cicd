package com.airwallex.data.streaming.common.UDFS;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class Split extends TableFunction<Tuple2<String, Integer>> {

    @Override
    public void open(FunctionContext context) throws Exception {
        // access "hashcode_factor" parameter
        // "12" would be the default value if parameter does not exist
        separator = context.getJobParameter("separator", ",");
    }

    private String separator = " ";


    public void eval(String str) {
        if(str.split(separator).length==2)
            return;
        for (String s : str.split(separator)) {
            // use collect(...) to emit a row
            collect(new Tuple2<String, Integer>(s, s.length()));
        }
    }



    private static void printTable(Table table, StreamTableEnvironment bsTableEnv) {
        System.out.println("===========");
        bsTableEnv.toAppendStream(table, Row.class).print();

    }
    public static class CustomTypeSplit extends TableFunction<Row> {
        public void eval(String str) {
            for (String s : str.split(",")) {
                Row row = new Row(2);
                row.setField(0, s);
                row.setField(1, s.length());
                collect(row);
            }
        }

        @Override
        public TypeInformation<Row> getResultType() {
            return Types.ROW(Types.STRING, Types.INT);
        }
    }
}
