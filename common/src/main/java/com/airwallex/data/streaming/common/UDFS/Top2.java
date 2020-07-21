package com.airwallex.data.streaming.common.UDFS;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableAggregateFunction;

/**
 * The following methods of TableAggregateFunction are required depending on the use case:
 * <p>
 * retract() is required for aggregations on bounded OVER windows.
 * merge() is required for many batch aggregations and session window aggregations.
 * resetAccumulator() is required for many batch aggregations.
 * emitValue() is required for batch and window aggregations.
 */
public class Top2 extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2.Top2Accum> {

    @Override
    public Top2Accum createAccumulator() {
        Top2Accum acc = new Top2Accum();
        acc.first = Integer.MIN_VALUE;
        acc.second = Integer.MIN_VALUE;
        acc.oldFirst = Integer.MIN_VALUE;
        acc.oldSecond = Integer.MIN_VALUE;
        return acc;
    }

    public void accumulate(Top2Accum acc, Integer v) {
        if (v > acc.first) {
            acc.second = acc.first;
            acc.first = v;
        } else if (v > acc.second) {
            acc.second = v;
        }
    }

    public void emitUpdateWithRetract(Top2Accum acc, RetractableCollector<Tuple2<Integer, Integer>> out) {
        if (!acc.first.equals(acc.oldFirst)) {
            // if there is an update, retract old value then emit new value.
            if (acc.oldFirst != Integer.MIN_VALUE) {
                out.retract(Tuple2.of(acc.oldFirst, 1));
            }
            out.collect(Tuple2.of(acc.first, 1));
            acc.oldFirst = acc.first;
        }

        if (!acc.second.equals(acc.oldSecond)) {
            // if there is an update, retract old value then emit new value.
            if (acc.oldSecond != Integer.MIN_VALUE) {
                out.retract(Tuple2.of(acc.oldSecond, 2));
            }
            out.collect(Tuple2.of(acc.second, 2));
            acc.oldSecond = acc.second;
        }
    }

    public class Top2Accum {
        public Integer first;
        public Integer second;
        public Integer oldFirst;
        public Integer oldSecond;
    }

}
