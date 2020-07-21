package com.airwallex.data.streaming.common.item;

public class SimpleCountItem {

    public String key;
    public long rowTime;

    public SimpleCountItem(String key, long rowTime) {
        this.key = key;
        this.rowTime = rowTime;
    }

    public SimpleCountItem(){

    }
}
