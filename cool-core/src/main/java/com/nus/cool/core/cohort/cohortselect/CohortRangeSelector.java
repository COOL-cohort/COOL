package com.nus.cool.core.cohort.cohortselect;

import com.nus.cool.core.cohort.filter.RangeFilter;
import com.nus.cool.core.cohort.storage.ProjectedTuple;
import com.nus.cool.core.cohort.storage.Scope;
import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.IntRangeField;
import com.nus.cool.core.field.RangeField;
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
  public Boolean selectAll() {
    return false;
  }

  @Override
  public String selectCohort(ProjectedTuple tuple, MetaChunkRS metaChunkRS) {
    return selectCohort(tuple.getValueBySchema(this.getSchema()));
  }

  private String selectCohort(FieldValue input) throws IllegalArgumentException {
    if (!(input instanceof RangeField)) {
      throw new IllegalArgumentException(
        "Invalid input for CohortRangeSelector (RangeField required)");
    }
    Scope s = new Scope(null, null);
    return filter.accept((RangeField) input, s) 
      ? s.getString(input instanceof IntRangeField) : null;
  }

  @Override
  public String getSchema() {
    return this.filter.getFilterSchema();
  }

  @Override
  public void loadMetaInfo(MetaChunkRS metachunk) {}

  @Override
  public Boolean maybeSkipMetaChunk(MetaChunkRS metachunk) {
    return false;
  }
}
