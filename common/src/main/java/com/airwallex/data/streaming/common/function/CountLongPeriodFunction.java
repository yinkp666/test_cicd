package com.airwallex.data.streaming.common.function;

import com.airwallex.data.streaming.common.item.SimpleCountItem;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CountLongPeriodFunction extends KeyedProcessFunction<Tuple, SimpleCountItem, Row> {

    private ListState<SimpleCountItem> itemState;

    private long slideWindowSize;

    private Logger logger = LoggerFactory.getLogger(CountLongPeriodFunction.class);

    public CountLongPeriodFunction(long slideWindowSize) {
        this.slideWindowSize = slideWindowSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ListStateDescriptor<SimpleCountItem> itemsStateDesc = new ListStateDescriptor<>(
                "itemState-state",
                SimpleCountItem.class);
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.days(7))
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        itemsStateDesc.enableTimeToLive(ttlConfig);
        itemState = getRuntimeContext().getListState(itemsStateDesc);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Row> out) throws Exception {

        List<SimpleCountItem> newStateList = new ArrayList<>();
        logger.info("onTimer time: "+timestamp);
        for(SimpleCountItem simpleCountItem : itemState.get()){
            long rowTime = simpleCountItem.rowTime;
            if(timestamp - rowTime < TimeUnit.MINUTES.toMillis(slideWindowSize)){
                newStateList.add(simpleCountItem);
            }
        }
        itemState.update(newStateList);

    }

    @Override
    public void processElement(SimpleCountItem value, Context ctx, Collector<Row> out) throws Exception {

        itemState.add(value);
        long lastRowTime = value.rowTime;

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(lastRowTime);
        calendar.set(Calendar.HOUR_OF_DAY, 23);
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.SECOND, 59);
        calendar.set(Calendar.MILLISECOND, 999);

        long fireTime = calendar.getTimeInMillis();
        ctx.timerService().registerProcessingTimeTimer(fireTime);

        Long cnt = 0L;
        for(SimpleCountItem simpleCountItem : itemState.get()){
            long rowTime = simpleCountItem.rowTime;
            if(lastRowTime - rowTime < TimeUnit.MINUTES.toMillis(slideWindowSize)){
                cnt++;
            }
        }
        Row row = new Row(2);
        row.setField(0,value.key);
        row.setField(1,cnt);
        out.collect(row);

    }
}
