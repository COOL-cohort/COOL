package com.nus.cool.core.cohort.refactor.cohortselect;

import com.nus.cool.core.cohort.refactor.filter.Filter;
import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.io.readstore.MetaChunkRS;

/**
 * Interface for different CohortSelector 
 */
public interface CohortSelector {
    public String selectCohort(ProjectedTuple tuple, MetaChunkRS metaChunkRS);
    
    public String getSchema();

    public Filter getFilter();

}
