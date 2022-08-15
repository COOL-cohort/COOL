package com.nus.cool.core.cohort.refactor.aggregate;

import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.storage.RetUnit;

public class MaxAggregate implements AggregateFunc {

    private final AggregateType type = AggregateType.MAX;

    private String schema;

    public MaxAggregate(String schema){
        this.schema = schema;
    }

    @Override
    public void calculate(RetUnit retUnit, ProjectedTuple tuple) {
        int parse_value = (Integer) tuple.getValueBySchema(this.schema);
        float value =  (float) parse_value;
        if (retUnit.getValue() < value) {
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
        // TODO Auto-generated method stub
        return null;
    }

}
