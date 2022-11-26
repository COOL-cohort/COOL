package com.nus.cool.core.cohort.aggregate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Enum class for aggregate type.
 */
public enum AggregateType {
  AVERAGE("AVERAGE"), COUNT("COUNT"), MAX("MAX"), MIN("MIN"), SUM("SUM"), DISTINCT("DISTINCT");

  private final String text;

  private AggregateType(final String text) {
    this.text = text;
  }

  @JsonValue
  @Override
  public String toString() {
    return text;
  }

  /**
   * return AggregateType named by string.
   */
  @JsonCreator
  public static AggregateType forValue(String str) {
    switch (str) {
      case "AVERAGE":
        return AVERAGE;
      case "COUNT":
        return COUNT;
      case "MAX":
        return MAX;
      case "MIN":
        return MIN;
      case "SUM":
        return SUM;
      case "DISTINCT":
        return DISTINCT;
      default:
        throw new IllegalArgumentException();
    }
  }
}
