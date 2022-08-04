package com.nus.cool.core.cohort.refactor.filter;

import java.util.List;
import java.util.ArrayList;
import java.util.BitSet;

import com.google.common.base.Preconditions;
import com.nus.cool.core.cohort.refactor.storage.Scope;

import lombok.Getter;

/**
 * Range filter class
 */
public class RangeFilter implements Filter {

    // Some static defined parameter
    private static final FilterType type = FilterType.Range;
    private static final String MinLimit = "MIN";
    private static final String MaxLimit = "MAX";
    @Getter
    private static final String splitChar = "-";

    // accepted range
    @Getter
    private List<Scope> acceptRangeList;

    // filter schema
    private String fieldSchema;

    public RangeFilter(String fieldSchema, String[] acceptValues) {
        this.fieldSchema = fieldSchema;
        this.acceptRangeList = new ArrayList<Scope>();
        for (int i = 0; i < acceptValues.length; i++) {
            acceptRangeList.add(parse(acceptValues[i]));
        }
    }

    /**
     * Need add Scope in the next steps
     * @param fieldSchema
     */
    public RangeFilter(String fieldSchema, List<Scope> scopeList){
        this.fieldSchema = fieldSchema;
        this.acceptRangeList = scopeList; 
    }


    @Override
    public Boolean accept(String value) throws RuntimeException {
        throw new UnsupportedOperationException("RangeFilter dosent't implement the Integer accept method");
    }

    @Override
    public Boolean accept(Integer value) throws RuntimeException {
        for (Scope u : acceptRangeList) {
            if (u.IsInScope(value)) {
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
        for (int i = 0; i < values.size(); i++) {
            if (accept(values.get(i))) {
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
     * 
     * @param str
     * @return RangeUnit
     */
    private  Scope parse(String str) {
        String[] part = str.split(splitChar);
        Preconditions.checkArgument(part.length == 2,
                "Split RangeUnit failed");
        Integer l = null, r = null;
        if (!part[0].equals(MinLimit)) {
            l = Integer.parseInt(part[0]);
        }
        if (!part[1].equals(MaxLimit)) {
            r = Integer.parseInt(part[1]);
        }
        return new Scope(l, r);
    }

    @Override
    public String getFilterSchema() {
        return this.fieldSchema;
    }

}
