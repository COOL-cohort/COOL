package com.nus.cool.core.cohort.refactor.utils;

import com.google.common.base.Preconditions;
import com.nus.cool.core.cohort.refactor.utils.TimeUtils.TimeUnit;

import lombok.Getter;

public class TimeWindow implements Comparable<TimeWindow>{

    @Getter
    private long length;
    @Getter
    private TimeUnit unit;

    public TimeWindow(long length, TimeUnit unit){
        this.length = length;
        this.unit = unit;
    }

    @Override
    public int compareTo(TimeWindow arg) {
        Preconditions.checkArgument(arg.unit == this.unit, 
            "Different TimeUnit isn't allowed to compare");
        return Long.compare(this.length, arg.length);
    }
}
