package com.airwallex.data.streaming.common.function;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class CheckExistInListFunction extends ProcessFunction<Tuple, Row> {

    private ValueState<Boolean> isExistState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Boolean> existStateDesc = new ValueStateDescriptor<Boolean>(
                "exist-value-state",
                Boolean.class
        );
        isExistState = getRuntimeContext().getState(existStateDesc);
    }


    @Override
    public void processElement(Tuple value, Context ctx, Collector<Row> out) throws Exception {

        String key = value.getField(0);
        Row row = new Row(2);
        if (isExistState.value() == null) {
            isExistState.update(true);
            row.setField(0,key);
            row.setField(1,false);
            row.setField(2,value);
        }
        else if (isExistState.value()) {
            row.setField(0,key);
            row.setField(1,true);
            row.setField(2,value);
        }
        out.collect(row);
    }
}
