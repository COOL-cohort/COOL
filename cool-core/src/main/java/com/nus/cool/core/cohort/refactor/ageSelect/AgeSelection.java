package com.nus.cool.core.cohort.refactor;

import java.util.Calendar;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Store Age condition and filter valid age for cohort Analysis.
 */
public class AgeSelection {
    // fieldSchema should be fixed to actionTime
    private final String fieldSchema = "actionTime";

    private TimeUtils.TimeUnit unit;

    private Integer min, max;

    // For current implementation, we ignore different interval for age
    @JsonIgnore
    private final int interval = 1;

    /**
     * After read from json directly
     * if min and max is null, we should initialize it with the extremum
     */
    public void init() {
        if (this.min == null)
            this.min = Integer.MIN_VALUE;
        if (this.max == null)
            this.max = Integer.MAX_VALUE;
    }

    /**
     * According to the ageSelection, return the age of this action tuple
     * 
     * @param birthDate
     * @param actionTime
     * @return if the age is out of range return -1 else return age
     *         TODO(lingze): Long to int will raise loss of precision,
     *         future implementation should focus on long type data.
     */
    public int generateAge(Calendar birthDate, Calendar actionTime) {
        TimeWindow tw = DateUtils.getDifference(birthDate, actionTime, this.unit);
        int age = (int) tw.getLength();
        age = age > max || age < min ? -1 : age;
        return age;
    }
}
