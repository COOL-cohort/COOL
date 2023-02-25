package com.nus.cool.core.cohort.aggregate;

import com.nus.cool.core.cohort.storage.ProjectedTuple;
import com.nus.cool.core.cohort.storage.RetUnit;
import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.RangeField;

/**
 * Sum aggregator.
 */
public class SumAggregate implements AggregateFunc {

  private final AggregateType type = AggregateType.SUM;

  private String schema;

  public SumAggregate(String schema) {
    this.schema = schema;
  }

  @Override
  public void calculate(RetUnit retUnit, ProjectedTuple tuple) {
    FieldValue fv = tuple.getValueBySchema(this.schema);
    if (!(fv instanceof RangeField)) {
      throw new IllegalArgumentException("Aggregation requires range field");
    }
    float value = ((RangeField) fv).getFloat();
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
