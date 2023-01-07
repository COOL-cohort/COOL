package com.nus.cool.core.cohort.aggregate;

import com.nus.cool.core.cohort.storage.ProjectedTuple;
import com.nus.cool.core.cohort.storage.RetUnit;

/**
 * Average aggregator.
 */
public class AverageAggregate implements AggregateFunc {

  private final AggregateType type = AggregateType.AVERAGE;

  private String schema;

  public AverageAggregate(String schema) {
    this.schema = schema;
  }

  @Override
  public void calculate(RetUnit retUnit, ProjectedTuple tuple) {
    float value = tuple.getValueBySchema(this.schema).getFloat();
    float sum = retUnit.getValue() * retUnit.getCount() + value;
    retUnit.setCount(retUnit.getCount() + 1);
    retUnit.setValue(sum / retUnit.getCount());
  }

  @Override
  public AggregateType getType() {
    return this.type;
  }

  @Override
  public String getSchema() {
    return this.schema;
  }
}
