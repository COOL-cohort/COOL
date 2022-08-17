package com.nus.cool.core.cohort.refactor.aggregate;

import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.storage.RetUnit;

public class MinAggregate implements AggregateFunc {

    private final AggregateType type = AggregateType.MIN;

    private String schema;

    public MinAggregate(String schema) {
        this.schema = schema;
    }

    @Override
    public void calculate(RetUnit retUnit, ProjectedTuple tuple) {
        int parse_value = (Integer) tuple.getValueBySchema(this.schema);
        float value =  (float) parse_value;
        if(!retUnit.isUsed()){
            retUnit.setValue(value);
            retUnit.setUsed(true);
        } else if (retUnit.getValue() > value) {
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
