package com.nus.cool.core.cohort.refactor.cohortSelect;

public interface CohortSelector {
    public String selectCohort(Object input);
    
    public String getSchema();
}
