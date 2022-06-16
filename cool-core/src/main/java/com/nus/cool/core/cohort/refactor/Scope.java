package com.nus.cool.core.cohort.refactor;

/**
 * represent a range from left to right
 * [left, right]
 */
public class Scope {
    private Integer left, right;

    public Scope(Integer l, Integer r){
        this.left = l == null ? Integer.MIN_VALUE: l; 
        this.right = r == null ? Integer.MAX_VALUE : r;
    }

    public Boolean IsInScope(Integer i){
        return i <= this.right && i >= this.left;
    }
}
