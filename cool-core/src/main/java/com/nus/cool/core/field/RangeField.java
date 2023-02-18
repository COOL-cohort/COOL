package com.nus.cool.core.field;

/**
 * Range field interface.
 */
public interface RangeField extends FieldValue {
  @Override
  int compareTo(FieldValue o) throws IllegalArgumentException;
}
