package com.nus.cool.core.cohort.aggregate;

import com.nus.cool.core.cohort.storage.ProjectedTuple;
import com.nus.cool.core.cohort.storage.RetUnit;

/**
 * Max aggregator.
 */
public class MaxAggregate implements AggregateFunc {

  private final AggregateType type = AggregateType.MAX;

  private String schema;

  public MaxAggregate(String schema) {
    this.schema = schema;
  }

  @Override
  public void calculate(RetUnit retUnit, ProjectedTuple tuple) {
    float value = tuple.getValueBySchema(this.schema).getFloat();
    retUnit.setCount(retUnit.getCount() + 1);

    if (!retUnit.isUsed()) {
      retUnit.setValue(value);
      retUnit.setUsed(true);
    } else if (retUnit.getValue() < value) {
      retUnit.setValue(value);
    }
  }

  @Override
  public AggregateType getType() {
    return this.type;
  }

  @Override
  public String getSchema() {
    // TODO Auto-generated method stub
    return null;
  }
}
