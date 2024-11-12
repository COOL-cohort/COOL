package com.nus.cool.core.cohort;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/** Enum for result type. */
public enum ResultType {
  FLOAT("FLOAT"), INT("INT");

  private final String text;

  private ResultType(final String text) {
    this.text = text;
  }

  @JsonValue
  @Override
  public String toString() {
    return text;
  }

  /**
   * return ResultType named by string.
   */
  @JsonCreator
  public static ResultType forValue(String str) {
    switch (str) {
      case "FLOAT":
        return FLOAT;
      case "INT":
        return INT;
      default:
        throw new IllegalArgumentException();
    }
  }
}
