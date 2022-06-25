package com.nus.cool.core.cohort.refactor.aggregate;

import com.nus.cool.core.cohort.refactor.storage.RetUnit;

public class MinFunc implements AggregateFunc {

    private final AggregateType type = AggregateType.MIN;

    @Override
    public void calulate(RetUnit retUnit, float value) {
        if (retUnit.getValue() > value) {
            retUnit.setValue(value);
        }
        retUnit.setCount(retUnit.getCount() + 1);
    }

    @Override
    public AggregateType getType() {
        return this.type;
    }

}
