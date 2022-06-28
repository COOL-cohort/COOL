package com.nus.cool.core.cohort.refactor.aggregate;

import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.storage.RetUnit;

public class SumFunc implements AggregateFunc {

    private final AggregateType type = AggregateType.SUM;

    private String schema;
    @Override
    public void calulate(RetUnit retUnit, ProjectedTuple tuple) {
        float value = (float)tuple.getValueBySchema(this.schema);
        retUnit.setValue(retUnit.getValue() + value);
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
