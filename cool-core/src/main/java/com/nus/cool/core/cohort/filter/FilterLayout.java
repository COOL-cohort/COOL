package com.nus.cool.core.cohort.filter;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;

/**
 * Read from json file into FilterLayout.
 * Parse to construct different filter instance
 **/
public class FilterLayout {
  @Getter
  @Setter
  private String fieldSchema;

  @Getter
  private FilterType type;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Getter
  private String[] acceptValue;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Getter
  private String[] rejectValue;

  @Override
  public String toString() {
    String acStr = "";
    String rejStr = "";
    if (this.acceptValue != null) {
      acStr = acceptValue.toString();
    }
    if (this.rejectValue != null) {
      rejStr = rejectValue.toString();
    }
    return String.format(
        "FilterLayout fieldSchema %s filter type %s, acceptValue %s, rejectValue %s",
        fieldSchema, type, acStr, rejStr);
  }

  /**
   * Generate different filter instances.
   */
  public Filter generateFilter() {
    switch (type) {
      case Range:
        Preconditions.checkArgument(rejectValue == null,
            "For RangeFilter, rejectValue property is null");
        return new RangeFilter(fieldSchema, acceptValue);
      case Set:
        if (this.acceptValue != null) {
          return new SetAcceptFilter(fieldSchema, acceptValue);
        }
        if (this.rejectValue != null) {
          return new SetRejectFilter(fieldSchema, rejectValue);
        }
        return null;
      default:
        throw new IllegalArgumentException(
            String.format("No filter of this type named %s", type));
    }
  }

  /**
   * Default Constructor, JsonMapper use it.
   */
  public FilterLayout() {
  }

  // ----------------- For Test ----------------------- //
  /**
   * Constructor for filterlayout.
   *
   * @param isSet   whether SetFilter
   * @param acList  accept List
   * @param rejList reject List
   */
  public FilterLayout(Boolean isSet, String[] acList, String[] rejList) {
    if (isSet) {
      this.type = FilterType.Set;
    } else {
      this.type = FilterType.Range;
    }
    this.acceptValue = acList;
    this.rejectValue = rejList;
  }
}