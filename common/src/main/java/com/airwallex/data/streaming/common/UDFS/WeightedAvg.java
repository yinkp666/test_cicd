package com.airwallex.data.streaming.common.UDFS;

import org.apache.flink.table.functions.AggregateFunction;

import java.util.Iterator;

/**
 * The following methods of AggregateFunction are required depending on the use case:
 *
 * retract() is required for aggregations on bounded OVER windows.
 * merge() is required for many batch aggregations and session window aggregations.
 * resetAccumulator() is required for many batch aggregations.
 */
public class WeightedAvg extends AggregateFunction<Long, WeightedAvg.WeightedAvgAccum> {
    public static class WeightedAvgAccum {
        public long sum = 0;
        public int count = 0;
    }

    @Override
    public WeightedAvgAccum createAccumulator() {
        return new WeightedAvgAccum();
    }

    @Override
    public Long getValue(WeightedAvgAccum acc) {
        if (acc.count == 0) {
            return null;
        } else {
            return acc.sum / acc.count;
        }
    }

    public void accumulate(WeightedAvgAccum acc, long iValue, int iWeight) {
        acc.sum += iValue * iWeight;
        acc.count += iWeight;
    }

    public void retract(WeightedAvgAccum acc, long iValue, int iWeight) {
        acc.sum -= iValue * iWeight;
        acc.count -= iWeight;
    }

    public void merge(WeightedAvgAccum acc, Iterable<WeightedAvgAccum> it) {
        Iterator<WeightedAvgAccum> iter = it.iterator();
        while (iter.hasNext()) {
            WeightedAvgAccum a = iter.next();
            acc.count += a.count;
            acc.sum += a.sum;
        }
    }

    public void resetAccumulator(WeightedAvgAccum acc) {
        acc.count = 0;
        acc.sum = 0L;
    }


}

