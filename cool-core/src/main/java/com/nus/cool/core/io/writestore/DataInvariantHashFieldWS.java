package com.nus.cool.core.io.writestore;

import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.schema.FieldType;
import java.io.DataOutput;
import java.io.IOException;

/**
 * we should keep meta here for data chunk filter.
 */
public class DataInvariantHashFieldWS implements DataFieldWS {

  private FieldType fieldType;

  // private final MetaFieldWS metaFieldWS;

  public DataInvariantHashFieldWS(FieldType fieldType, MetaFieldWS metaField) {
    this.fieldType = fieldType;
    // this.metaFieldWS = metaField;
  }

  @Override
  public int writeTo(DataOutput out) throws IOException {
    return 0;
  }

  @Override
  public FieldType getFieldType() {
    return this.fieldType;
  }

  @Override
  public void put(FieldValue value) throws IllegalArgumentException {
    // // for invariant data field, no need to write data
  }
}
