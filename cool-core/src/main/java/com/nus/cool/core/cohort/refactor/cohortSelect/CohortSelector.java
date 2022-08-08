package com.nus.cool.core.cohort.refactor.cohortSelect;

import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;

/**
 * Interface for different CohortSelector 
 */
public interface CohortSelector {
    public String selectCohort(ProjectedTuple tuple);
    
    public String getSchema();
}
