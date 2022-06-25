package com.nus.cool.core.cohort.refactor.aggregate;

import com.nus.cool.core.cohort.refactor.storage.RetUnit;

public class AverageFunc implements AggregateFunc {

    private final AggregateType type = AggregateType.AVERAGE;


    @Override
    public void calulate(RetUnit retUnit, int value) {
        float sum = retUnit.getValue() * retUnit.getCount() + (float)value;
        retUnit.setCount(retUnit.getCount() + 1);
        retUnit.setValue(sum/retUnit.getCount());
    }

    @Override
    public AggregateType getType() {
        return this.type;
    }

}
