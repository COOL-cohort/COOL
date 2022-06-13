package com.nus.cool.core.cohort.refactor;

import com.nus.cool.core.cohort.refactor.TimeUtils.TimeUnit;

public class TimeWindow {
    public long length;
    public TimeUnit unit;

    public TimeWindow(long length, TimeUnit unit){
        this.length = length;
        this.unit = unit;
    }
}
