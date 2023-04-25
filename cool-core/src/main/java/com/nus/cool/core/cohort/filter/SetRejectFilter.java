package com.nus.cool.core.cohort.filter;

/**
 * Reject SetFilter.
 */
public class SetRejectFilter extends SetFilter {

  public SetRejectFilter(String fieldSchema, String[] rejectValues) {
    super(fieldSchema, rejectValues);
  }

  @Override
  public Boolean accept(Integer value) throws IllegalStateException {
    if (this.gidSet == null) {
      throw new IllegalStateException("Filter is not initialized");
    }
    return !this.gidSet.contains(value);
  }

  @Override
  public Boolean accept(String value) {
    return !this.valueSet.contains(value);
  }
}
