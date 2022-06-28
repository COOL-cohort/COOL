package com.nus.cool.core.cohort.refactor.aggregate;

import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.storage.RetUnit;

public class AverageAggregate implements AggregateFunc {

    private final AggregateType type = AggregateType.AVERAGE;

    private String schema;

    public AverageAggregate(String schema) {
        this.schema = schema;
    }

    @Override
    public void calulate(RetUnit retUnit, ProjectedTuple tuple) {
        float value = (float) tuple.getValueBySchema(this.schema);
        float sum = retUnit.getValue() * retUnit.getCount() + value;
        retUnit.setCount(retUnit.getCount() + 1);
        retUnit.setValue(sum / retUnit.getCount());
    }

    @Override
    public AggregateType getType() {
        return this.type;
    }

    @Override
    public String getSchema() {
        return this.schema;
    }

}
