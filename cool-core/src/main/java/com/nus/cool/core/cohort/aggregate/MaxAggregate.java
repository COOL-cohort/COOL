package com.nus.cool.core.cohort.aggregate;

import com.nus.cool.core.cohort.storage.ProjectedTuple;
import com.nus.cool.core.cohort.storage.RetUnit;
import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.RangeField;

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
    FieldValue fv = tuple.getValueBySchema(this.schema);
    if (!(fv instanceof RangeField)) {
      throw new IllegalArgumentException("Aggregation requires range field");
    }
    float value = ((RangeField) fv).getFloat();
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
