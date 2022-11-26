package com.nus.cool.core.cohort.filter;

import com.nus.cool.core.cohort.storage.Scope;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import java.util.BitSet;
import java.util.List;

/**
 * All filter class.
 */
public class AllFilter implements Filter {

  private static final FilterType type = FilterType.ALL;


  /**
   * Accept all values.
   *
   * @param value the Input Value to check
   * @return true
   */
  @Override
  public Boolean accept(Integer value) {
    return true;
  }

  /**
   *  Accept all values.
   *
   * @param values the batch of input value to check
   * @return null
   */
  @Override
  public BitSet accept(List<Integer> values) {
    return null;
  }

  /**
   *  Accept all values.
   *
   * @param values Scope of time_min-time_max
   * @return true
   */
  @Override
  public boolean accept(Scope values) {
    return true;
  }

  /**
   *  Accept all values.
   *
   * @param value value
   * @return true
   */
  @Override
  public Boolean accept(String value) {
    return true;
  }

  /**
   *  Accept all values.
   *
   * @param values values
   * @return null
   */
  @Override
  public BitSet accept(String[] values) {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * return AllFilter.
   *
   * @return type the type of filter.
   */
  @Override
  public FilterType getType() {
    return type;
  }

  /**
   * No schema to return.
   *
   * @return null.
   */
  @Override
  public String getFilterSchema() {
    return null;
  }

  @Override
  public void loadMetaInfo(MetaChunkRS metaChunkRS) {
  // for all Filter, no need to load info
  }
}
