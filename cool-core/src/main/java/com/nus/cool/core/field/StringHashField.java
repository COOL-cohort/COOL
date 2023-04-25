package com.nus.cool.core.field;

import com.nus.cool.core.schema.FieldType;
import com.rabinhash.RabinHashFunction32;
import lombok.AllArgsConstructor;

/**
 * Range field with underlying int value.
 */
@AllArgsConstructor
public class StringHashField implements HashField {

  private final String val;

  @Override
  public FieldType getType() {
    return FieldType.Segment;
  }
  
  private static final RabinHashFunction32 rhash = RabinHashFunction32.DEFAULT_HASH_FUNCTION;

  
  @Override
  public boolean checkEqual(Object o) {
    return ((o instanceof StringHashField) && val.equals(((StringHashField) o).val));
  }

  @Override
  public int getInt() {
    return rhash.hash(val);
  }

  @Override
  public int compareTo(FieldValue o) throws IllegalArgumentException {
    if (!(o instanceof StringHashField)) {
      throw new IllegalArgumentException("Invalid type to compare against IntRangeField");
    }
    return val.compareTo(((StringHashField) o).val);
  }

  @Override
  public String getString() {
    return val;
  }

  @Override
  public String toString() {
    return val;
  }
}
