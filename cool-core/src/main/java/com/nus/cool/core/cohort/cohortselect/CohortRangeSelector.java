package com.nus.cool.core.cohort.cohortselect;

import com.nus.cool.core.cohort.filter.Filter;
import com.nus.cool.core.cohort.filter.RangeFilter;
import com.nus.cool.core.cohort.storage.ProjectedTuple;
import com.nus.cool.core.cohort.storage.Scope;
import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import java.util.List;

/**
 * Class CohortRangeSelector for Range type column schema.
 * It helps to judge whether the value in cohortSchema is acceptable
 */
public class CohortRangeSelector implements CohortSelector {

  private RangeFilter filter;

  public CohortRangeSelector(String fieldSchema, List<Scope> scopeList) {
    this.filter = new RangeFilter(fieldSchema, scopeList);
  }

  @Override
  public String selectCohort(ProjectedTuple tuple, MetaChunkRS metaChunkRS) {
    return selectCohort(tuple.getValueBySchema(this.getSchema()));
  }

  private String selectCohort(FieldValue input) {
    Integer i = input.getInt();
    for (Scope u : this.filter.getAcceptRangeList()) {
      if (u.isInScope(i)) {
        return u.toString();
      }
    }
    return null;
  }

  public String getSchema() {
    return this.filter.getFilterSchema();
  }

  @Override
  public Filter getFilter() {
    return filter;
  }

}
