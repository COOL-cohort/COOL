package com.nus.cool.core.field;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class IntRangeField implements RangeField {
  
  private final int val;

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
  public int compareTo(Object o) throws IllegalArgumentException {
    if (!(o instanceof IntRangeField)) {
      throw new IllegalArgumentException("Invalid type to compare against IntRangeField");
    }
    return val - ((IntRangeField) o).val;
  }

  @Override
  public String getString() {
    return String.valueOf(val);
  }
}
