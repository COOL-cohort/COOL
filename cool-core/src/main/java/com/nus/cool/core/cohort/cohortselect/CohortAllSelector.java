package com.nus.cool.core.cohort.cohortselect;

import com.nus.cool.core.cohort.filter.AllFilter;
import com.nus.cool.core.cohort.filter.Filter;
import com.nus.cool.core.cohort.storage.ProjectedTuple;
import com.nus.cool.core.io.readstore.MetaChunkRS;

/**
 * Class CohortAllSelector for one cohort.
 * It accepts all values in cohortSchema
 */
public class CohortAllSelector implements CohortSelector {

  private AllFilter filter;

  public CohortAllSelector() {
    this.filter = new AllFilter();
  }
  /**
   * select cohort.
   * @param tuple tuple.
   * @param metaChunkRS metaChunkRS.
   * @return name of cohort
   */
  @Override
  public String selectCohort(ProjectedTuple tuple, MetaChunkRS metaChunkRS) {
    return "all";
  }

  /**
   * @return null
   */
  @Override
  public String getSchema() {
    return null;
  }

  /**
   * @return null
   */
  @Override
  public Filter getFilter() {
    return filter;
  }
}
