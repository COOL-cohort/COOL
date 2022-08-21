package com.nus.cool.core.cohort.refactor.aggregate;

import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.storage.RetUnit;

public class MaxAggregate implements AggregateFunc {

    private final AggregateType type = AggregateType.MAX;

    private static class MaxAggregateSingularHolder {
        private static final MaxAggregate instance = new MaxAggregate();
    }

    private MaxAggregate() {
    }

    public static final MaxAggregate getInstance() {
        return MaxAggregateSingularHolder.instance;
    }

    @Override
    public void calculate(RetUnit retUnit, ProjectedTuple tuple, String schema) {
        int parse_value = (Integer) tuple.getValueBySchema(schema);
        float value = (float) parse_value;
        retUnit.setCount(retUnit.getCount() + 1);

        if (!retUnit.isUsed()) {
            retUnit.setValue(value);
            retUnit.setUsed(true);
        } else if (retUnit.getValue() < value) {
            retUnit.setValue(value);
        }
    }

    @Override
    public AggregateType getType() {
        return this.type;
    }

}
