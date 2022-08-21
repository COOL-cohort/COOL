package com.nus.cool.core.cohort.refactor.aggregate;

import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.storage.RetUnit;

public class AverageAggregate implements AggregateFunc {

    private final AggregateType type = AggregateType.AVERAGE;

    private static class AverageAggregateSingularHolder {
        private static final AverageAggregate instance = new AverageAggregate();
    }

    private AverageAggregate() {
    }

    public static final AverageAggregate getInstance() {
        return AverageAggregateSingularHolder.instance;
    }

    @Override
    public void calculate(RetUnit retUnit, ProjectedTuple tuple, String schema) {
        int parse_value = (Integer) tuple.getValueBySchema(schema);
        float value = (float) parse_value;
        float sum = retUnit.getValue() * retUnit.getCount() + value;
        retUnit.setCount(retUnit.getCount() + 1);
        retUnit.setValue(sum / retUnit.getCount());
    }

    @Override
    public AggregateType getType() {
        return this.type;
    }

}
