package com.nus.cool.core.cohort.refactor;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nus.cool.core.cohort.refactor.birthselect.BirthSelectionLayout;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import lombok.Getter;

/**
 * Funnel query.
 */

@Getter
public class FunnelQuery {

  private String dataSource;

  private String inputCohort;

  @JsonProperty("birthSelector")
  private List<BirthSelectionLayout> birthSelectionLayout = new ArrayList<>();

  boolean isOrdered = true;

  public List<BirthSelectionLayout> getStages() {
    return birthSelectionLayout;
  }

  public void setStages(List<BirthSelectionLayout> stages) {
    this.birthSelectionLayout = stages;
  }

  public boolean isOrdered() {
    return isOrdered;
  }

  public void setOrdered(boolean isOrdered) {
    this.isOrdered = isOrdered;
  }

  public String getDataSource() {
    return dataSource;
  }

  public void setDataSource(String dataSource) {
    this.dataSource = dataSource;
  }

  public String getInputCohort() {
    return inputCohort;
  }

  public void setInputCohort(String inputCohort) {
    this.inputCohort = inputCohort;
  }

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
