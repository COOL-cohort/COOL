package com.nus.cool.core.cohort.refactor.aggregate;


import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.storage.RetUnit;

public class CountAggregate implements AggregateFunc {

    private final AggregateType type = AggregateType.COUNT;

    public CountAggregate() {

    }

    @Override
    public void calulate(RetUnit retUnit, ProjectedTuple tuple) {
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
