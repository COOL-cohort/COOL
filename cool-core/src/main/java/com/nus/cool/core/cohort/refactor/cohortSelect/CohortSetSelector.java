package com.nus.cool.core.cohort.refactor.cohortSelect;
import com.nus.cool.core.cohort.refactor.filter.SetFilter;
import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;

public class CohortSetSelector extends SetFilter implements CohortSelector {

    
    public CohortSetSelector(String fieldSchema, String[] acceptValues, String[] rejectedValues) {
        super(fieldSchema, acceptValues, rejectedValues);
    }

    public String selectCohort(Object input) {
        String s = String.valueOf(input);
        if(this.accept(s)) return s;
        return null;
    }

    public String getSchema(){
        return this.getFilterSchema();
    }

    @Override
    public String selectCohort(ProjectedTuple tuple) {
        return selectCohort(tuple.getValueBySchema(this.getSchema()));
    }
    
}
