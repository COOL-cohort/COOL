package com.nus.cool.core.cohort.refactor.ageselect;

import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.nus.cool.core.cohort.refactor.storage.Scope;
import com.nus.cool.core.cohort.refactor.utils.DateUtils;
import com.nus.cool.core.cohort.refactor.utils.TimeUtils;
import com.nus.cool.core.cohort.refactor.utils.TimeWindow;

/**
 * Store Age condition and filter valid age for cohort Analysis.
 */
public class AgeSelection {
    private TimeUtils.TimeUnit unit;

    private Scope scope;

    @JsonIgnore
    public final static int DefaultNullAge = -1;

    public AgeSelection(Scope scope, TimeUtils.TimeUnit unit) {
        this.scope = scope;
        this.unit = unit;
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
    public int generateAge(LocalDateTime birthDate, LocalDateTime actionTime) {
        TimeWindow tw = DateUtils.getDifference(birthDate, actionTime, this.unit);
        int age = (int) tw.getLength();
        if (this.scope.isInScope(age)) {
            return age;
        }
        return -1;
    }
}
