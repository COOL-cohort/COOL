package com.nus.cool.core.cohort.aggregate;

import com.nus.cool.core.cohort.storage.ProjectedTuple;
import com.nus.cool.core.cohort.storage.RetUnit;

/**
 * Count aggregator.
 */
public class CountAggregate implements AggregateFunc {

  private final AggregateType type = AggregateType.COUNT;

  public CountAggregate() {

  }

  @Override
  public void calculate(RetUnit retUnit, ProjectedTuple tuple) {
    retUnit.setCount(retUnit.getCount() + 1);
    retUnit.setValue(retUnit.getCount());
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
