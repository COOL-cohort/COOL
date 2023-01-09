package com.nus.cool.core.field;

import lombok.AllArgsConstructor;

/**
 * Range field with underlying int value.
 */
@AllArgsConstructor
public class StringHashField implements HashField {

  private final String val;
  
  @Override
  public boolean checkEqual(Object o) {
    return ((o instanceof StringHashField) && val.equals(((StringHashField) o).val));
  }

  @Override
  public int getInt() {
    return 0;
  }

  @Override
  public float getFloat() {
    return 0;
  }

  @Override
  public String getString() {
    return val;
  }
}
