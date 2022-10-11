package com.nus.cool.core.cohort.refactor;

import java.util.HashMap;
import java.util.HashSet;

// import com.nus.cool.core.cohort.refactor.aggregate.AggregateFunc;

public class CohortRetCollection {

    // structure
    private int min,max,interval;
    
    private String schema;

    // private List<AggregateFunc> aggregateList;

    private HashMap<String, HashSet<String>> cohortUserSet;

    public CohortRetCollection(int min, int max, int interval){
        this.min = min;
        this.max = max;
        this.interval = interval;
    }
}
