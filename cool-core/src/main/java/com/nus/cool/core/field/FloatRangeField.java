package com.nus.cool.core.field;

import lombok.AllArgsConstructor;

/**
 * Range field with underlying float value.
 */
@AllArgsConstructor
public class FloatRangeField implements RangeField {
  
  private final float val;

  @Override
  public boolean checkEqual(Object o) {
    return ((o instanceof FloatRangeField) && val == ((FloatRangeField) o).val);
    
  }

  @Override
  public int getInt() {
    return (int) val;
  }

  @Override
  public float getFloat() {
    return val;
  }

  @Override
  public int compareTo(FieldValue o) throws IllegalArgumentException {
    if (!(o instanceof FloatRangeField)) {
      throw new IllegalArgumentException("Invalid type to compare against FloatRangeField");
    }
    float oVal = ((FloatRangeField) o).val;
    if (val == oVal) {
      return 0;
    }
    return (val < oVal) ? -1 : 1;
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
