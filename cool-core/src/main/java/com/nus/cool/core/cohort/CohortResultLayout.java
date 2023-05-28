package com.nus.cool.core.cohort;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.jsonschema.JsonSerializableSchema;
import java.util.ArrayList;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Layout class to facilitate json serialization of cohort query.
 */
@Data
@JsonSerializableSchema
public class CohortResultLayout {

  @AllArgsConstructor
  @JsonSerializableSchema
  private static class CohortResInfo {
    @JsonProperty("cohortName")
    private String cohortName;
    @JsonProperty("cohortSize")
    private int cohortSize;
  }

  private ArrayList<CohortResInfo> cohortResList = new ArrayList<>();

  /**
   * Adding a cohort res.
   *
   * @param cohortName cohortName
   * @param cohortSize cohortSize
   */
  public void addOneCohortRes(String cohortName, int cohortSize) {
    CohortResInfo cRes = new CohortResInfo(cohortName, cohortSize);
    this.cohortResList.add(cRes);
  }
}
