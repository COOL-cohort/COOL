package com.nus.cool.core.io.storevector;

import com.nus.cool.core.field.HashField;

/**
 * Input vector interface for hash field.
 */
public interface HashFieldInputVector {
  HashField getValue(int idx);
}
