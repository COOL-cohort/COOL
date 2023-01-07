package com.nus.cool.core.cohort.aggregate;

import com.nus.cool.core.cohort.storage.ProjectedTuple;
import com.nus.cool.core.cohort.storage.RetUnit;

/**
 * Distinct count aggregator.
 */
public class DistinctCountAggregate implements AggregateFunc {

  private final AggregateType type = AggregateType.DISTINCT;

  private String schema;

  public DistinctCountAggregate(String schema) {
    this.schema = schema;
  }

  @Override
  public void calculate(RetUnit retUnit, ProjectedTuple tuple) {
    int gid = tuple.getValueBySchema(this.schema).getInt();
    if (!retUnit.getUserIdSet().contains(gid)) {
      retUnit.getUserIdSet().add(gid);
      retUnit.setValue(retUnit.getValue() + 1);
    }
    retUnit.setCount(retUnit.getCount() + 1);
  }

  @Override
  public AggregateType getType() {
    return this.type;
  }

  @Override
  public String getSchema() {
    return null;
  }

}