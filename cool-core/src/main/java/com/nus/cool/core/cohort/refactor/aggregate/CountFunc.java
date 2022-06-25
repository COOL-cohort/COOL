package com.nus.cool.core.cohort.refactor.aggregate;

import com.nus.cool.core.cohort.refactor.storage.RetUnit;


public class CountFunc implements AggregateFunc{

    private final AggregateType  type = AggregateType.COUNT;

    @Override
    public void calulate(RetUnit retUnit, float value) {
        retUnit.setCount(retUnit.getCount() + 1);
    }

    @Override
    public AggregateType getType() {
        return this.type;
    }
    
}
