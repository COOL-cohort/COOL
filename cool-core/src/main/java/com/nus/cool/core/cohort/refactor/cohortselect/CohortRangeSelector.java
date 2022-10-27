package com.nus.cool.core.cohort.refactor.cohortSelect;


import java.util.List;

import com.nus.cool.core.cohort.refactor.filter.Filter;
import com.nus.cool.core.cohort.refactor.filter.RangeFilter;
import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.storage.Scope;
import com.nus.cool.core.io.readstore.MetaChunkRS;

/**
 * Class CohortRangeSelector for Range type column schema
 * It helps to judge whether the value in cohortSchema is acceptable
 */
public class CohortRangeSelector implements CohortSelector  {

    private RangeFilter filter;

    public CohortRangeSelector(String fieldSchema, List<Scope> scopeList){
        this.filter = new RangeFilter(fieldSchema, scopeList);
    }

    private String selectCohort(Object input) {
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
    public String selectCohort(ProjectedTuple tuple, MetaChunkRS metaChunkRS) {
        return selectCohort(tuple.getValueBySchema(this.getSchema()));
    }

    @Override
    public Filter getFilter(){
        return filter;
    }

}
