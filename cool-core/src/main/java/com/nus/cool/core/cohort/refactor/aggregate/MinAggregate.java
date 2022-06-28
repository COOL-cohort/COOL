package com.nus.cool.core.cohort.refactor.aggregate;

import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.storage.RetUnit;

public class MinAggregate implements AggregateFunc {

    private final AggregateType type = AggregateType.MIN;

    private String schema; 
    
    public MinAggregate(String schema){
        this.schema =schema;
    }
    @Override
    public void calulate(RetUnit retUnit, ProjectedTuple tuple) {
        float value = (float) tuple.getValueBySchema(this.schema);
        if (retUnit.getValue() > value) {
            retUnit.setValue(value);
        }
        retUnit.setCount(retUnit.getCount() + 1);
    }

    @Override
    public AggregateType getType() {
        return this.type;
    }

    @Override
    public String getSchema() {
        return null;
    }

}
