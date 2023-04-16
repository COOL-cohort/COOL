package com.nus.cool.core.field;

/**
 * Range field interface.
 */
public interface RangeField extends FieldValue {
  float getFloat();
  
  @Override
  int compareTo(FieldValue o) throws IllegalArgumentException;

  @Override
  boolean equals(Object obj);
}
