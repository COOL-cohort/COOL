package com.nus.cool.core.cohort.filter;

/**
 * Accept Set Filter.
 */
public class SetAcceptFilter extends SetFilter {

  public SetAcceptFilter(String fieldSchema, String[] acceptValues) {
    super(fieldSchema, acceptValues);
  }

  @Override
  public Boolean accept(Integer value) throws IllegalStateException {
    if (this.gidSet == null) {
      throw new IllegalStateException("Filter is not initialized");
    }
    return this.gidSet.contains(value);
  }

  @Override
  public Boolean accept(String value) {
    return this.valueSet.contains(value);
  }
}
