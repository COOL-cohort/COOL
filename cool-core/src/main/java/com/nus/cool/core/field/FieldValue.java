package com.nus.cool.core.field;

/**
 * Field value abstract the values stored and handled in COOL.
 */
public interface FieldValue extends Comparable<FieldValue> {
  boolean checkEqual(Object o);
  
  int getInt();

  float getFloat();

  // serialize to string as output
  String getString(); 
}
