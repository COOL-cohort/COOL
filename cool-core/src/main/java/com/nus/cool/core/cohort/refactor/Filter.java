package com.nus.cool.core.cohort.refactor;

import java.util.BitSet;
import java.util.List;

public interface Filter {

    /**
     * Set Filter implements this function
     * @param value the Input Value to check
     * @return true if the condition is met, otherwise false
     */ 
    public Boolean accept(String value) throws RuntimeException;

    /**
     * Range Filter implements this function
     * @param value the Input Value to check
     * @return true if the condition is met, otherwise false
     */ 
    public Boolean accept(Integer value) throws RuntimeException;
    
    /**
     * Set Filter implements this function 
     * @param values the batch of input value to check
     * @return List of bool stored in BitSet
     */
    public BitSet accpet(List<String> values) throws RuntimeException;
    
    /**
     * Range Filter implements this function 
     * @param values the batch of input value to check
     * @return List of bool stored in BitSet
     */
    public BitSet accept(List<Integer> values) throws RuntimeException;

    /**
     * @return the type of Filter
     */
    public FilterType getType();

}

