package com.nus.cool.core.field;

public interface RangeField extends FieldValue {
  int compareTo(Object o) throws IllegalArgumentException;
}
