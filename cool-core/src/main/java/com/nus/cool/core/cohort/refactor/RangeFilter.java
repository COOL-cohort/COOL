package com.nus.cool.core.cohort.refactor;

import java.util.List;
import java.util.ArrayList;
import java.util.BitSet;

import com.google.common.base.Preconditions;

import lombok.Getter;

public class RangeFilter implements Filter {

    // Some static defined parameter
    private static final FilterType type = FilterType.Range; 
    private static final String MinLimit = "min";
    private static final String MaxLimit = "max";
    private static final String splitChar = "-";

    // accepted range 
    private ArrayList<Scope> acceptRangeList = new ArrayList<>();
    
    @Getter
    private String fieldSchema;
    

    public RangeFilter(String fieldSchema,String[] acceptValues){
        this.fieldSchema = fieldSchema;
        for(int i = 0; i < acceptValues.length; i++){
            acceptRangeList.add(parse(acceptValues[i]));
        }
    }

    @Override
    public Boolean accept(String value) throws RuntimeException {
        throw new UnsupportedOperationException("RangeFilter dosent't implement the Integer accept method");
    }

    @Override
    public Boolean accept(Integer value) throws RuntimeException {
        for(Scope u : acceptRangeList) {
            if(u.IsInScope(value)){
                return true;
            }
        }
        return false;
    }

    @Override
    public BitSet accpet(List<String> values) throws RuntimeException {
        throw new UnsupportedOperationException("RangeFilter dosent't implement the Integer accept method");
    }

    @Override
    public BitSet accept(List<Integer> values) throws RuntimeException {
        BitSet res = new BitSet(values.size());
        for (int i = 0 ; i < values.size(); i++){
            if (accept(values.get(i))){
                res.set(i);
            }
        }
        return res;
    }
    
    @Override
    public FilterType getType() {
        return type;
    }

    /**
     * Parse string to RangeUnit
     * Exmaple [145 - 199] = RangeUnit{left:145 , right:199}
     * @param str
     * @return RangeUnit
     */
    private Scope parse(String str){
        String[] part = str.split(splitChar);     
        Preconditions.checkArgument(part.length == 2, 
            "Split RangeUnit failed");   
        Integer l = null,r = null;
        if (part[0] != MinLimit) {
            l = Integer.parseInt(part[0]);
        }
        if (part[1] != MaxLimit) {
            r = Integer.parseInt(part[1]);
        }
        return new Scope(l, r);
    }

    
}
