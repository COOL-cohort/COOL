package com.nus.cool.core.cohort.aggregate;

import com.nus.cool.core.cohort.storage.ProjectedTuple;
import com.nus.cool.core.cohort.storage.RetUnit;
import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.RangeField;

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
  public void calculate(RetUnit retUnit, ProjectedTuple tuple)
      throws IllegalArgumentException {
    FieldValue fv = tuple.getValueBySchema(this.schema);
    if (!(fv instanceof RangeField)) {
      throw new IllegalArgumentException("Aggregation requires range field");
    }
    float value = ((RangeField) fv).getFloat();
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
