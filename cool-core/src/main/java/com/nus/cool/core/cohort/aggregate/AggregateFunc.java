package com.nus.cool.core.cohort.aggregate;

import com.nus.cool.core.cohort.storage.ProjectedTuple;
import com.nus.cool.core.cohort.storage.RetUnit;

/**
 * The Interface for different aggregator.
 */
public interface AggregateFunc {

  /**
   * Modify retUnit in place.
   */
  public void calculate(RetUnit retUnit, ProjectedTuple tuple) throws IllegalArgumentException;

  public AggregateType getType();

  public String getSchema();
}
