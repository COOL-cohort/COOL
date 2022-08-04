package com.nus.cool.core.cohort.refactor.cohortSelect;


import java.util.List;

import com.nus.cool.core.cohort.refactor.filter.RangeFilter;
import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.storage.Scope;

/**
 * Class CohortRangeSelector extends RangeFilter
 * It helps to judge whether the value in cohortSchema is acceptable
 */
public class CohortRangeSelector implements CohortSelector  {

    private RangeFilter filter;

    public CohortRangeSelector(String fieldSchema, List<Scope> scopeList){
        this.filter = new RangeFilter(fieldSchema, scopeList);
    }

    public String selectCohort(Object input) {
        Integer i = (Integer)input;
        for(Scope u : this.filter.getAcceptRangeList()){
            if(u.IsInScope(i)) return u.toString();
        }
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
