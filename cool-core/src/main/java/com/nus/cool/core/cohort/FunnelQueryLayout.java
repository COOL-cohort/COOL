package com.nus.cool.core.cohort;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nus.cool.core.cohort.birthselect.BirthSelectionLayout;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import lombok.Getter;

/**
 * Funnel query.
 */

@Getter
public class FunnelQueryLayout {

  @JsonProperty("dataSource")
  private String dataSource;

  @JsonProperty("inputCohort")
  private String inputCohort;

  @JsonProperty("stages")
  private List<BirthSelectionLayout> birthSelectionLayout = new ArrayList<>();

  /**
   * Check validity of the funnel query.
   */
  @JsonIgnore
  public boolean isValid() throws IOException {
    if ((birthSelectionLayout != null) && (dataSource != null)) {
      return true;
    } else {
      throw new IOException("[x] Invalid funnel query.");
    }
  }

  /**
   * Get the schema related to the funnel analysis.
   *
   * @return the schema related to the funnel analysis
   */
  public HashSet<String> getFunnelSchemaSet() {
    HashSet<String> ret = new HashSet<>();
    for (int i = 0; i < this.birthSelectionLayout.size(); i++) {
      ret.addAll(this.birthSelectionLayout.get(i).getRelatedSchemas());
    }
    return ret;
  }


}
