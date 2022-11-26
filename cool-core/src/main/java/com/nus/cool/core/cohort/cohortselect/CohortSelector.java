package com.nus.cool.core.cohort.cohortselect;

import com.nus.cool.core.cohort.filter.Filter;
import com.nus.cool.core.cohort.storage.ProjectedTuple;
import com.nus.cool.core.io.readstore.MetaChunkRS;

/**
 * Interface for different CohortSelector.
 */
public interface CohortSelector {
  public String selectCohort(ProjectedTuple tuple, MetaChunkRS metaChunkRS);

  public String getSchema();

  public Filter getFilter();

}
