package com.nus.cool.core.cohort.aggregate;

import com.nus.cool.core.cohort.storage.ProjectedTuple;
import com.nus.cool.core.cohort.storage.RetUnit;
import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.RangeField;

/**
 * Min aggregator.
 */
public class MinAggregate implements AggregateFunc {

  private final AggregateType type = AggregateType.MIN;

  private String schema;

  public MinAggregate(String schema) {
    this.schema = schema;
  }

  @Override
  public void calculate(RetUnit retUnit, ProjectedTuple tuple) {
    FieldValue fv = tuple.getValueBySchema(this.schema);
    if (!(fv instanceof RangeField)) {
      throw new IllegalArgumentException("Aggregation requires range field");
    }
    float value = ((RangeField) fv).getFloat();
    if (!retUnit.isUsed()) {
      retUnit.setValue(value);
      retUnit.setUsed(true);
    } else if (retUnit.getValue() > value) {
      retUnit.setValue(value);
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
