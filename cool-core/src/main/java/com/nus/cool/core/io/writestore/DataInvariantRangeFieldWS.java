package com.nus.cool.core.io.writestore;

import com.nus.cool.core.schema.FieldType;
import java.io.DataOutput;
import java.io.IOException;



/**
 * whether to record the field's meta data in this chunk (min,max).
 * it can speed up the cohort processing
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
  public void put(String tuple) throws IOException {
    // TODO Auto-generated method stub
  }

}
