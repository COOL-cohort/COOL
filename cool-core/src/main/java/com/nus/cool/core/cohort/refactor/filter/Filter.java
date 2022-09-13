package com.nus.cool.core.cohort.refactor.filter;

import com.nus.cool.core.cohort.refactor.storage.Scope;

import java.util.BitSet;
import java.util.List;

/**
 * All filters should implement filter's interface.
 */
public interface Filter {

  /**
   * Set Filter implements this function.
   * 
   * @param value the Input Value to check
   * @return true if the condition is met, otherwise false
   */
  public Boolean accept(String value) throws RuntimeException;

  /**
   * Range Filter implements this function.
   * 
   * @param value the Input Value to check
   * @return true if the condition is met, otherwise false
   */
  public Boolean accept(Integer value) throws RuntimeException;

  /**
   * Set Filter implements this function.
   * 
   * @param values the batch of input value to check
   * @return List of bool stored in BitSet
   */
  public BitSet accept(String[] values) throws RuntimeException;

  /**
   * Range Filter implements this function.
   * 
   * @param values the batch of input value to check
   * @return List of bool stored in BitSet
   */
  public BitSet accept(List<Integer> values) throws RuntimeException;

  /**
   * Range Filter implements this function.
   * 
   * @param values Scope of time_min-time_max
   * @return List of bool stored in BitSet
   */
  public BitSet accept(Scope values) throws RuntimeException;

  /**
   * Return the type of Filter.
   */
  public FilterType getType();

  /**
   * Return the related Schema.
   */
  public String getFilterSchema();

}
