package com.nus.cool.core.cohort.refactor.filter;

import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import com.google.common.base.Preconditions;
import java.lang.UnsupportedOperationException;

public class SetFilter implements Filter {

    private static final FilterType type = FilterType.Set;

    // O(1) to check whether the value is acceptable
    private HashSet<String> acceptSet;

    private HashSet<String> rejectSet;

    private String fieldSchema;


    protected SetFilter(String fieldSchema,String[] acceptValues, String[] rejectedValues) {
        Preconditions.checkArgument(acceptValues!=null || rejectedValues !=null, 
            "acceptValues and rejectValue can not be null at the same time");
        
        this.fieldSchema = fieldSchema;
        if (acceptValues != null) {
            this.acceptSet = new HashSet<>();
            for(int i = 0; i < acceptValues.length; i++){
                this.acceptSet.add(acceptValues[i]);
            }
        }

        if (rejectedValues!=null) {
            this.rejectSet = new HashSet<>();
            for(int i = 0 ; i< rejectedValues.length; i++){
                this.rejectSet.add(rejectedValues[i]);
            }
        }
        // Precondition to check no overlap element between acceptSet and rejectSet
    }

    @Override
    public Boolean accept(String value) throws RuntimeException {
        if(this.acceptSet!= null){
            return this.acceptSet.contains(value);
        } else if(this.rejectSet != null){
            return !this.rejectSet.contains(value);
        }
        throw new RuntimeException("acList and rejList can not be null at the same time");
    }

    @Override
    public Boolean accept(Integer value) throws RuntimeException {
        throw new UnsupportedOperationException("SetFilter dosent't implement the Integer accept method");
    }

    @Override
    public BitSet accpet(List<String> values) throws RuntimeException {
        BitSet res = new BitSet(values.size());
        for(int i = 0 ; i < values.size(); i++){
            if (this.accept(values.get(i))) {
                res.set(i);
            }
        }
        return res;
    }

    @Override
    public BitSet accept(List<Integer> values) throws RuntimeException {
        throw new UnsupportedOperationException("SetFilter dosent't implement the Integer accept method");
    }

    @Override
    public FilterType getType() {
        return type;
    }

    /* ---------------------- Helper function ---------------*/

    @Override
    public String getFilterSchema() {
        return this.fieldSchema;
    }

 
    
}
