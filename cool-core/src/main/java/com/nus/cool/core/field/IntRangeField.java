package com.nus.cool.core.field;

import com.nus.cool.core.schema.FieldType;
import lombok.AllArgsConstructor;

/**
 * Range field with underlying int value.
 */
@AllArgsConstructor
public class IntRangeField implements RangeField {
  
  private final int val;

  @Override
  public FieldType getType() {
    return FieldType.Metric;
  }

  @Override
  public boolean checkEqual(Object o) {
    return ((o instanceof IntRangeField) && val == ((IntRangeField) o).val);
  }

  @Override
  public int getInt() {
    return val;
  }

  @Override
  public float getFloat() {
    return val;
  }
  
  @Override
  public int compareTo(FieldValue o) throws IllegalArgumentException {
    if (!(o instanceof RangeField)) {
      throw new IllegalArgumentException("Invalid type to compare against IntRangeField");
    }
    float oVal = ((RangeField) o).getFloat();
    if (getFloat() == oVal) {
      return 0;
    }
    return getFloat() < oVal ? -1 : 1;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof IntRangeField)) {
      return false;
    }
    return val == ((IntRangeField) o).val;
  }

  @Override
  public String getString() {
    return String.valueOf(val);
  }

  @Override
  public String toString() {
    return getString();
  }
}
