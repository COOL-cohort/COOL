package com.nus.cool.core.cohort.refactor.aggregate;

import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.storage.RetUnit;

public class SumAggregate implements AggregateFunc {

    private final AggregateType type = AggregateType.SUM;

    private static class SumAggregateSingularHolder{
        private static final SumAggregate instance = new SumAggregate();
    }

    private SumAggregate(){}

    public static final SumAggregate getInstance(){
        return SumAggregateSingularHolder.instance;
    }
    
    @Override
    public void calculate(RetUnit retUnit, ProjectedTuple tuple, String schema) {
        int parse_value = (Integer) tuple.getValueBySchema(schema);
        float value =  (float) parse_value;
        retUnit.setValue(retUnit.getValue() + value);
        retUnit.setCount(retUnit.getCount() + 1);
    }

    @Override
    public AggregateType getType() {
        return this.type;
    }
}
