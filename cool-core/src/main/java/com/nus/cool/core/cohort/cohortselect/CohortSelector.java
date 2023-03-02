package com.nus.cool.core.cohort.cohortselect;

import com.nus.cool.core.cohort.storage.ProjectedTuple;
import com.nus.cool.core.io.readstore.MetaChunkRS;

/**
 * Interface for different CohortSelector.
 */
public interface CohortSelector {

  public Boolean selectAll();
  
  public String selectCohort(ProjectedTuple tuple, MetaChunkRS metaChunkRS);

  public String getSchema();

  /**
   * Initialize filters with meta chunk info.
   */
  public void loadMetaInfo(MetaChunkRS metachunk);

  /**
   * Determine if we can skip the cubelet.
   *
   * @return If none of the values in the Cublet will be accepted by the filter.
   */
  public Boolean maybeSkipMetaChunk(MetaChunkRS metachunk);

  // public Filter getFilter();

  /**
   * Generate a selector that selects all values on the field. 
   *
   * @param fieldSchema the field on which selector opertes
   * @return the selector
   */
  public static CohortSelector generateAllSelector(String fieldSchema) {
    return new CohortSelector() {
      @Override
      public Boolean selectAll() {
        return true;
      }
      
      @Override
      public String selectCohort(ProjectedTuple tuple, MetaChunkRS metaChunkRS) {
        return "all";
      }

      @Override
      public String getSchema() {
        return fieldSchema;
      }

      @Override
      public void loadMetaInfo(MetaChunkRS metachunk) {}

      @Override
      public Boolean maybeSkipMetaChunk(MetaChunkRS metachunk) {
        return false;
      }
    };
  }
}
