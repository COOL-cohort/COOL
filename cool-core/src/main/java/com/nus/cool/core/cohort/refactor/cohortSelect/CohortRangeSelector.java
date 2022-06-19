package com.nus.cool.core.cohort.refactor.cohortSelect;


import com.nus.cool.core.cohort.refactor.filter.RangeFilter;
import com.nus.cool.core.cohort.refactor.storage.Scope;


public class CohortRangeSelector extends RangeFilter implements CohortSelector  {

    private CohortRangeSelector(String fieldSchema){
        super(fieldSchema);
    }

    public static CohortRangeSelector generateCohortRangeSelector(String fieldSchema, int max, int min, int interval) {
        CohortRangeSelector selector = new CohortRangeSelector(fieldSchema);
        for(int i = min; i< max; i+= interval){
            int uplevel = i+interval > max ? max : i + interval;
            Scope u = new Scope(i, uplevel);
            selector.addScope(u);
        }
        return selector;
    }

    @Override
    public String selectCohort(Object input) {
        Integer i = (Integer)input;
        for(Scope u : this.acceptRangeList){
            if(u.IsInScope(i)) return i.toString();
        }
        return null;
    }

    public String getSchema(){
        return this.getFilterSchema();
    }
    
}
