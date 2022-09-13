package com.nus.cool.core.cohort.refactor.aggregate;

import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.storage.RetUnit;

/**
 * The Interface for different aggregator.
 */
public interface AggregateFunc {
    
  /**
  * Modify retUnit in place.
  */
  public void calculate(RetUnit retUnit, ProjectedTuple tuple);
    
  public AggregateType getType();

  public String getSchema();
}
