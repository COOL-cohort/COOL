package com.nus.cool.core.field;

/**
 * Boxing values into FieldValue.
 */
public class ValueWrapper {
  public static IntRangeField of(int v) {
    return new IntRangeField(v);
  }

  public static FloatRangeField of(float v) {
    return new FloatRangeField(v);
  }
  
  public static StringHashField of(String v) {
    return new StringHashField(v);
  }
}
