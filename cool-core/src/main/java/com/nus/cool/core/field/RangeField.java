package com.nus.cool.core.field;

/**
 * Range field interface.
 */
public interface RangeField extends FieldValue {
  int compareTo(Object o) throws IllegalArgumentException;
}
