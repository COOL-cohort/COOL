package com.nus.cool.core.cohort.refactor.cohortselect;

import com.nus.cool.core.cohort.refactor.filter.Filter;
import com.nus.cool.core.cohort.refactor.filter.SetFilter;
import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;

/**
 * Class CohortSetSelector for set type column schema. It helps to judge whether
 * the value in cohortSchema is acceptable
 */
public class CohortSetSelector implements CohortSelector {

  private final SetFilter filter;

  public CohortSetSelector(String fieldSchema, String[] acceptValues, String[] rejectedValues) {
    this.filter = new SetFilter(fieldSchema, acceptValues, rejectedValues);
  }

  /**
   * Select the cohort based on the set field value.
   */
  public String selectCohort(Object input) {
    String s = String.valueOf(input);
    if (this.filter.accept(s)) {
      return s;
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
