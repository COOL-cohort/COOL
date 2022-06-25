package com.nus.cool.core.cohort.refactor.aggregate;

import com.nus.cool.core.cohort.refactor.storage.RetUnit;

public interface AggregateFunc {
    
    /**
     * modify retUnit in place
     * @param retUnit
     * @param value
     */
    public void calulate(RetUnit retUnit, float value);
    
    public AggregateType getType();
}
