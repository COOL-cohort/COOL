package com.nus.cool.core.cohort.refactor.cohortselect;

import com.nus.cool.core.cohort.refactor.filter.Filter;
import com.nus.cool.core.cohort.refactor.filter.RangeFilter;
import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.storage.Scope;
import java.util.List;

/**
 * Class CohortRangeSelector for Range type column schema It helps to judge
 * whether the value in cohortSchema is acceptable.
 */
public class CohortRangeSelector implements CohortSelector {

  private RangeFilter filter;

  public CohortRangeSelector(String fieldSchema, List<Scope> scopeList) {
    this.filter = new RangeFilter(fieldSchema, scopeList);
  }

  /**
   * Select the cohort based on the range field value.
   */
  public String selectCohort(Object input) {
    Integer i = (Integer) input;
    for (Scope u : this.filter.getAcceptRangeList()) {
      if (u.isInScope(i)) {
        return u.toString();
      }
    }
    return null;
  }

  @Override
  public String selectCohort(ProjectedTuple tuple) {
    return selectCohort(tuple.getValueBySchema(this.getSchema()));
  }

  @Override
  public String getSchema() {
    return this.filter.getFilterSchema();
  }

  @Override
  public Filter getFilter() {
    return filter;
  }

}
