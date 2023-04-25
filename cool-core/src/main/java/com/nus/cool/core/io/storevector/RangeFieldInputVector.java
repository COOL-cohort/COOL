package com.nus.cool.core.io.storevector;

import com.nus.cool.core.field.RangeField;

/**
 * Input vector interface for hash field.
 */
public interface RangeFieldInputVector {
  RangeField getValue(int idx);
}
