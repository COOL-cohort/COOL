package com.nus.cool.core.cohort.refactor.aggregate;

import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.storage.RetUnit;

public class DistinctCountAggregate implements AggregateFunc {

    private final AggregateType type = AggregateType.DISTINCT;

    private static class DistinctCountAggregateSingularHolder {
        private static final DistinctCountAggregate instance = new DistinctCountAggregate();
    }

    private DistinctCountAggregate() {
    }

    public static final DistinctCountAggregate getInstance() {
        return DistinctCountAggregateSingularHolder.instance;
    }

    @Override
    public void calculate(RetUnit retUnit, ProjectedTuple tuple, String schema) {
        String value = (String) tuple.getValueBySchema(schema);
        if (!retUnit.getUserIdSet().contains(value)) {
            retUnit.getUserIdSet().add(value);
            retUnit.setValue(retUnit.getValue() + 1);
        }
        retUnit.setCount(retUnit.getCount() + 1);
    }

    @Override
    public AggregateType getType() {
        return this.type;
    }

}
