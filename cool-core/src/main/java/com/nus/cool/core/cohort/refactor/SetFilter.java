package com.nus.cool.core.cohort.refactor;

import java.util.BitSet;
import java.util.HashSet;
import java.util.List;

import com.google.common.base.Preconditions;
import lombok.Getter;

import java.lang.UnsupportedOperationException;

public class SetFilter implements Filter {

    private static final FilterType type = FilterType.Set;

    // O(1) to check whether the value is acceptable
    private HashSet<String> acceptSet;

    private HashSet<String> rejectSet;

    @Getter
    private String fieldSchema;


    public SetFilter(String fieldSchema,String[] acceptValues, String[] rejectedValues) {
        Preconditions.checkArgument(acceptValues!=null&&rejectedValues!=null, 
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
    }

    @Override
    public Boolean accept(String value) throws RuntimeException {
        return !IsReject(value) && IsAccept(value);
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

    private boolean IsReject(String value){
        return this.rejectSet == null || !this.rejectSet.contains(value);
    }

    private boolean IsAccept(String value){
        return this.acceptSet != null && this.acceptSet.contains(value);
    }

 
    
}
