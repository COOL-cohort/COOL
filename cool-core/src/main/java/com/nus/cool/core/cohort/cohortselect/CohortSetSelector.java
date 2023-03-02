package com.nus.cool.core.cohort.cohortselect;

import com.nus.cool.core.cohort.filter.SetAcceptFilter;
import com.nus.cool.core.cohort.filter.SetFilter;
import com.nus.cool.core.cohort.filter.SetRejectFilter;
import com.nus.cool.core.cohort.storage.ProjectedTuple;
import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;

/**
 * Class CohortSetSelector for set type column schema.
 * It helps to judge whether the value in cohortSchema is acceptable
 */
public class CohortSetSelector implements CohortSelector {

  private final SetFilter filter;

  /**
   * Construct a cohort selector on a field.
   *
   * @param fieldSchema the field to define cohort
   * @param acceptValues accepted values on the field
   * @param rejectedValues rejected values on the field.
   */
  public CohortSetSelector(String fieldSchema, String[] acceptValues, String[] rejectedValues) {
    if (acceptValues != null) {
      this.filter = new SetAcceptFilter(fieldSchema, acceptValues);
    } else if (rejectedValues != null) {
      this.filter = new SetRejectFilter(fieldSchema, rejectedValues);
    } else {
      this.filter = SetFilter.generateEmptySetFilter(fieldSchema);
    }
  }

  @Override
  public Boolean selectAll() {
    return false;
  }

  @Override
  public String selectCohort(ProjectedTuple tuple, MetaChunkRS metaChunkRS) {
    FieldValue gid = selectCohort(tuple.getValueBySchema(this.getSchema()));
    if (gid == null) {
      throw new RuntimeException("GlobalID of Selected Cohort Should Exist in Cublet");
    }
    MetaFieldRS metaField = metaChunkRS.getMetaField(this.filter.getFilterSchema());
    return metaField.get(gid.getInt()).map(FieldValue::getString).orElse(null);
  }

  private FieldValue selectCohort(FieldValue input) {
    if (this.filter.accept(input)) {
      return input;
    }
    return null;
  }

  @Override
  public String getSchema() {
    return this.filter.getFilterSchema();
  }

  @Override
  public void loadMetaInfo(MetaChunkRS metachunk) {
    filter.loadMetaInfo(metachunk);
  }

  @Override
  public Boolean maybeSkipMetaChunk(MetaChunkRS metachunk) {
    return false;
  }
}
