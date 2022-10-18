package com.nus.cool.core.cohort.refactor.filter;

public class SetAcceptFilter extends SetFilter {

    public SetAcceptFilter(String fieldSchema, String[] acceptValues) {
        super(fieldSchema, acceptValues);
    }
    
    @Override
    public Boolean accept(Integer value) throws RuntimeException {
        if(this.gidSet == null)
            throw new RuntimeException("Filter is not initialized");
        return this.gidSet.contains(value);
    }
    
    @Override
    public Boolean accept(String value) throws RuntimeException {
        return this.valueSet.contains(value);       
    }
}
