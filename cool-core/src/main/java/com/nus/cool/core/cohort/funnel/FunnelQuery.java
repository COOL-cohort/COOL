package com.nus.cool.core.cohort.funnel;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.nus.cool.core.cohort.BirthSequence;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FunnelQuery {

  private String dataSource;

  private String inputCohort;

  private List<BirthSequence> stages = new ArrayList<>();

  boolean isOrdered = true;

  public List<BirthSequence> getStages() {
    return stages;
  }

  public void setStages(List<BirthSequence> stages) {
    this.stages = stages;
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
    if ((stages != null) && (dataSource != null)) {
      return true;
    } else {
      throw new IOException("[x] Invalid funnel query.");
    }
  }
}
