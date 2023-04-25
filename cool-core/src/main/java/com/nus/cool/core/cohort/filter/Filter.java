package com.nus.cool.core.cohort.filter;

import com.nus.cool.core.cohort.storage.Scope;
import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.io.readstore.MetaChunkRS;

/**
 * All filters should implement filter's interface.
 */
public interface Filter {

  /**
   * Filter a value.
   *
   * @param value this is the global id.
   * @return true if the condition is met, otherwise false
   */
  public Boolean accept(FieldValue value) throws IllegalArgumentException, IllegalStateException;

  /**
   * Get Filter type.
   *
   * @return the type of Filter
   */
  public FilterType getType();

  /**
   * Get related schema in filter.
   *
   * @return the related Schema
   */
  public String getFilterSchema();

  /**
   * Load meta information when get New Cublet.
   * Since the Gid is unique in every Cublet
   */
  public void loadMetaInfo(MetaChunkRS metaChunkRS);
}
