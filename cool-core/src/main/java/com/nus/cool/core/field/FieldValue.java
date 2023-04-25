package com.nus.cool.core.field;

import com.fasterxml.jackson.annotation.JsonValue;
import com.nus.cool.core.schema.FieldType;

/**
 * Field value abstract the values stored and handled in COOL.
 */
public interface FieldValue extends Comparable<FieldValue> {

  FieldType getType();

  boolean checkEqual(Object o);
  
  // for range field, the corresponding int is returned
  // for hash field, this is used to return the hash
  int getInt();

  // serialize to string as output
  @JsonValue
  String getString();
}
