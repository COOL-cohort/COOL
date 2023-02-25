package com.nus.cool.core.io.writestore;

import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.schema.FieldType;
import java.io.DataOutput;
import java.io.IOException;



/**
 * we should keep meta here for data chunk filter.
 */
public class DataInvariantRangeFieldWS implements DataFieldWS {

  private final FieldType fieldType;

  // private Integer min,max;

  public DataInvariantRangeFieldWS(FieldType fieldType) {
    this.fieldType = fieldType;

  }

  @Override
  public int writeTo(DataOutput out) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public FieldType getFieldType() {
    return this.fieldType;
  }

  @Override
  public void put(FieldValue tuple) throws IllegalArgumentException {
    // TODO Auto-generated method stub
  }
}
