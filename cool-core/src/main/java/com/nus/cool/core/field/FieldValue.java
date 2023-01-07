package com.nus.cool.core.field;

public interface FieldValue {
  boolean checkEqual(Object o);
  
  int getInt();

  float getFloat();

  // serialize to string as output
  String getString(); 
}
