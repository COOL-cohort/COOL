package com.nus.cool.core.cohort.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Enum class for filtertype.
 */
public enum FilterType {
  Range("RANGE"), Set("SET"), ALL("ALL");

  private final String text;

  private FilterType(final String text) {
    this.text = text;
  }

  @JsonValue
  @Override
  public String toString() {
    return text;
  }

  /**
   * return FilterType named by string.
   */
  @JsonCreator
  public static FilterType forValue(String value) {
    switch (value) {
      case "RANGE":
        return Range;
      case "SET":
        return Set;
      case "ALL":
        return ALL;
      default:
        throw new IllegalArgumentException();
    }
  }
}
