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
    @JsonProperty("name")
    private String name;
    @JsonProperty("cohortName")
    private String cohortName;
    @JsonProperty("cohortSize")
    private int cohortSize;
  }

  private CohortQueryLayout cohortQuery;
  private ArrayList<CohortResInfo> cohortResList = new ArrayList<>();

  /**
   * Adding a cohort res.
   *
   * @param name cohort file name
   * @param cohortName cohortName
   * @param cohortSize cohortSize
   */
  public void addOneCohortRes(String name, String cohortName, int cohortSize) {
    CohortResInfo cRes = new CohortResInfo(name, cohortName, cohortSize);
    this.cohortResList.add(cRes);
  }
}
