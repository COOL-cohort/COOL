package com.nus.cool.core.cohort.refactor.aggregate;

import java.util.HashSet;

import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.storage.RetUnit;

public class DistinctCountAggregate implements AggregateFunc {

    private final AggregateType type = AggregateType.DISTINCT;

    private String schema;

    private HashSet<String> set;

    public DistinctCountAggregate(String schema){
        this.schema = schema;
        this.set = new HashSet<>();
    }

    @Override
    public void calulate(RetUnit retUnit, ProjectedTuple tuple) {
        String value = (String)tuple.getValueBySchema(this.schema);
        if(!set.contains(value)){
            retUnit.setValue(retUnit.getValue() + 1);
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
