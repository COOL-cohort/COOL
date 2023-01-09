package com.nus.cool.core.io.storevector;

import com.nus.cool.core.field.IntRangeField;

/**
 * Input vector for int field.
 * Integer types is special in Cool, as it serves as the first level compression for Hash fields.
 */
public interface IntFieldInputVector extends RangeFieldInputVector {
  IntRangeField getValue(int idx);
}
