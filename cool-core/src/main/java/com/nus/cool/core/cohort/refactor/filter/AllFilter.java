package com.nus.cool.core.cohort.refactor.filter;

import com.nus.cool.core.cohort.refactor.storage.Scope;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import java.util.BitSet;
import java.util.List;

/**
 * All filter class.
 */
public class AllFilter implements Filter {

  private static final FilterType type = FilterType.ALL;


  /**
   * @param value the Input Value to check
   * @return true
   * @throws RuntimeException
   */
  @Override
  public Boolean accept(Integer value) throws RuntimeException {
    return true;
  }

  /**
   * @param values the batch of input value to check
   * @return
   * @throws RuntimeException
   */
  @Override
  public BitSet accept(List<Integer> values) throws RuntimeException {
    return null;
  }

  /**
   * @param values Scope of time_min-time_max
   * @return
   * @throws RuntimeException
   */
  @Override
  public boolean accept(Scope values) throws RuntimeException {
    return true;
  }

  /**
   * @param value value
   * @return
   * @throws RuntimeException
   */
  @Override
  public Boolean accept(String value) throws RuntimeException {
    return true;
  }

  /**
   * @param values values
   * @return
   * @throws RuntimeException
   */
  @Override
  public BitSet accept(String[] values) throws RuntimeException {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * return the type of filter.
   * @return type
   */
  @Override
  public FilterType getType() {
    return type;
  }

  /**
   * @return
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
