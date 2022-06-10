package com.nus.cool.core.cohort.refactor;

import java.util.List;
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedList;

import com.google.common.base.Preconditions;

import lombok.Getter;

public class RangeFilter implements Filter {

    // Some static defined parameter
    private static final FilterType type = FilterType.Range; 
    private static final String MinLimit = "min";
    private static final String MaxLimit = "max";
    private static final String splitChar = "-";

    // accepted range 
    private LinkedList<RangeUnit> acceptRangeList = new LinkedList<>();
    
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
        Iterator<RangeUnit> it = acceptRangeList.iterator();
        while(it.hasNext()){
            RangeUnit u = it.next();
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
    private RangeUnit parse(String str){
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
        return new RangeUnit(l, r);
    }



    /**
     *  Sub class to represent a scope
     */
    public class RangeUnit {
        private Integer left, right;

        public RangeUnit(Integer l, Integer r){
            Preconditions.checkArgument( !(l==null && r==null),
                "Left and right boudary can not be null in the same time");
            if(l!= null&&r!= null){
                Preconditions.checkArgument(r>=l, 
                    "Right Value should larger or equal to left Value");
            } 
            left = l;
            right = r;    
        }

        public Boolean IsInScope(Integer i){
            // left and right can not be null in the same time
            if (left == null) {
                return i <= right;
            } else  if (right == null) {
                return i >= left;
            } else {
                return left >= i && i <= right;
            }
        }
    }
    
}
