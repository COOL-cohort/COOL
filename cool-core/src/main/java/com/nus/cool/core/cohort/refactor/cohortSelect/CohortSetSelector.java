package com.nus.cool.core.cohort.refactor.cohortSelect;
import com.nus.cool.core.cohort.refactor.filter.SetFilter;
import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;

import lombok.Getter;

/**
 * Class CohortSetSelector extends SetFilter
 * It helps to judge whether the value in cohortSchema is acceptable
 */
public class CohortSetSelector implements CohortSelector {

    @Getter
    private SetFilter filter;
    
    public CohortSetSelector(String fieldSchema, String[] acceptValues, String[] rejectedValues) {
        this.filter = new SetFilter(fieldSchema, acceptValues, rejectedValues);
    }

    public String selectCohort(Object input) {
        String s = String.valueOf(input);
        if(this.filter.accept(s)) return s;
        return null;
    }

    public String getSchema(){
        return this.filter.getFilterSchema();
    }

    @Override
    public String selectCohort(ProjectedTuple tuple) {
        return selectCohort(tuple.getValueBySchema(this.getSchema()));
    }
    
}
