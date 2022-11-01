package com.nus.cool.core.cohort.refactor.filter;

public class SetRejectFilter extends SetFilter {

    public SetRejectFilter(String fieldSchema, String[] rejectValues) {
        super(fieldSchema, rejectValues);
    }

    @Override
    public Boolean accept(Integer value) throws RuntimeException {
        if (this.gidSet == null)
            throw new RuntimeException("Filter is not initialized");
        return !this.gidSet.contains(value);
        // return this.
    }

    @Override
    public Boolean accept(String value) throws RuntimeException {
        return !this.valueSet.contains(value);
    }

}
