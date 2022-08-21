package com.nus.cool.core.cohort.refactor.aggregate;

import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.storage.RetUnit;

public class MinAggregate implements AggregateFunc {

    private final AggregateType type = AggregateType.MIN;

    private static class MinAggregateSingularHolder {
        private static final MinAggregate instance = new MinAggregate();
    }

    private MinAggregate() {
    }

    public static final MinAggregate getInstance() {
        return MinAggregateSingularHolder.instance;
    }

    @Override
    public void calculate(RetUnit retUnit, ProjectedTuple tuple, String schema) {
        int parse_value = (Integer) tuple.getValueBySchema(schema);
        float value = (float) parse_value;
        if (!retUnit.isUsed()) {
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

}
