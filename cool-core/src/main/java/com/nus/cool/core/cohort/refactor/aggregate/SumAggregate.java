package com.nus.cool.core.cohort.refactor.aggregate;

import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.storage.RetUnit;

public class SumAggregate implements AggregateFunc {

  private final AggregateType type = AggregateType.SUM;

  private String schema;

  public SumAggregate(String schema) {
    this.schema = schema;
  }

  @Override
  public void calculate(RetUnit retUnit, ProjectedTuple tuple) {
    int parseValue = (Integer) tuple.getValueBySchema(this.schema);
    float value = (float) parseValue;
    retUnit.setValue(retUnit.getValue() + value);
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
