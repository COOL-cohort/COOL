package com.nus.cool.core.cohort.cohortselect;

import com.nus.cool.core.cohort.filter.Filter;
import com.nus.cool.core.cohort.filter.SetFilter;
import com.nus.cool.core.cohort.storage.ProjectedTuple;
import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;

/**
 * Class CohortSetSelector for set type column schema.
 * It helps to judge whether the value in cohortSchema is acceptable
 */
public class CohortSetSelector implements CohortSelector {

  private final Filter filter;

  public CohortSetSelector(String fieldSchema, String[] acceptValues, String[] rejectedValues) {
    this.filter = SetFilter.generateSetFilter(fieldSchema, acceptValues, rejectedValues);
  }

  @Override
  public String selectCohort(ProjectedTuple tuple, MetaChunkRS metaChunkRS) {
    Integer gid = selectCohort(tuple.getValueBySchema(this.getSchema()));
    if (gid == null) {
      throw new RuntimeException("GlobalID of Selected Cohort Should Exist in Cublet");
    }
    MetaFieldRS metaField = metaChunkRS.getMetaField(this.filter.getFilterSchema());
    return metaField.get(gid).map(FieldValue::getString).orElse(null);
  }

  private Integer selectCohort(FieldValue input) {
    int i = input.getInt();
    if (this.filter.accept(i)) {
      return i;
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
