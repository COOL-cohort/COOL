package com.nus.cool.core.cohort.refactor.aggregate;

import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.storage.RetUnit;

public class CountAggregate implements AggregateFunc {

    private final AggregateType type = AggregateType.COUNT;

    private static class CountAggregateSingularHolder {
        private static final CountAggregate instance = new CountAggregate();
    }

    private CountAggregate() {
    }

    public static final CountAggregate getInstance() {
        return CountAggregateSingularHolder.instance;
    }

    @Override
    public void calculate(RetUnit retUnit, ProjectedTuple tuple, String schema) {
        retUnit.setCount(retUnit.getCount() + 1);
        retUnit.setValue(retUnit.getCount());
    }

    @Override
    public AggregateType getType() {
        return this.type;
    }

}
