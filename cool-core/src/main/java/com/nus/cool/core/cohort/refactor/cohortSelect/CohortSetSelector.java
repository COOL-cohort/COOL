package com.nus.cool.core.cohort.refactor.cohortSelect;
import com.nus.cool.core.cohort.refactor.filter.SetFilter;

public class CohortSetSelector extends SetFilter implements CohortSelector {

    
    public CohortSetSelector(String fieldSchema, String[] acceptValues, String[] rejectedValues) {
        super(fieldSchema, acceptValues, rejectedValues);
    }

    @Override
    public String selectCohort(Object input) {
        String s = String.valueOf(input);
        if(this.accept(s)) return s;
        return null;
    }

    public String getSchema(){
        return this.getFilterSchema();
    }
    
}
