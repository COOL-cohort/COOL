package com.nus.cool.core.cohort.refactor.utils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public class TimeUtils {

  public enum TimeUnit {

    HOUR("HOUR"),

    DAY("DAY"),

    WEEK("WEEK"),

    MONTH("MONTH"),

    MINUTE("MINUTE"),

    SECOND("SECOND"),

    YEAR("YEAR");

    private final String text;

    private TimeUnit(final String text) {
      this.text = text;
    }

    @JsonValue
    @Override
    public String toString() {
      return text;
    }

    /**
     * Return time unit named by a string.
     */
    @JsonCreator
    public static TimeUnit forValue(String value) {
      switch (value) {
        case "HOUR":
          return TimeUnit.HOUR;
        case "DAY":
          return TimeUnit.DAY;
        case "WEEK":
          return TimeUnit.WEEK;
        case "MINUTE":
          return TimeUnit.MINUTE;
        case "SECOND":
          return TimeUnit.SECOND;
        case "YEAR":
          return TimeUnit.YEAR;
        case "MONTH":
          return TimeUnit.MONTH;
        default:
          throw new IllegalArgumentException();
      }
    }
  }

  public static TimeUnit generateTimeUnit(String str) {
    return TimeUnit.forValue(str);
  }
}
