package com.nus.cool.core.cohort.refactor.aggregate;

import com.nus.cool.core.cohort.refactor.storage.RetUnit;

public class SumFunc implements AggregateFunc {

    private final AggregateType type = AggregateType.SUM;

    @Override
    public void calulate(RetUnit retUnit, float value) {
        retUnit.setValue(retUnit.getValue() + value);
        retUnit.setCount(retUnit.getCount() + 1);
    }

    @Override
    public AggregateType getType() {
        return this.type;
    }

}
