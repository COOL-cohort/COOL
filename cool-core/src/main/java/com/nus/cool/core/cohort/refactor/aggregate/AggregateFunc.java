package com.nus.cool.core.cohort.refactor.aggregate;

import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.storage.RetUnit;

public interface AggregateFunc {
    
    /**
     * modify retUnit in place
     * @param retUnit
     * @param value
     */
    public void calulate(RetUnit retUnit, ProjectedTuple tuple);
    
    public AggregateType getType();

    public String getSchema();
}
