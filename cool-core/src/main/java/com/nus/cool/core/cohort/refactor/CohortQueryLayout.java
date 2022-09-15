package com.nus.cool.core.cohort.refactor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.refactor.ageselect.AgeSelectionLayout;
import com.nus.cool.core.cohort.refactor.birthselect.BirthSelectionLayout;
import com.nus.cool.core.cohort.refactor.cohortselect.CohortSelectionLayout;
import com.nus.cool.core.cohort.refactor.valueselect.ValueSelectionLayout;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import lombok.Getter;

/**
 * Layout class to facilitate json serialization of cohort query.
 */
@Getter
public class CohortQueryLayout {

  @JsonProperty("birthSelector")
  private BirthSelectionLayout birthSelectionLayout;

  @JsonProperty("cohortSelector")
  private CohortSelectionLayout cohortSelectionLayout;

  @JsonProperty("ageSelector")
  private AgeSelectionLayout agetSelectionLayout;

  @JsonProperty("valueSelector")
  private ValueSelectionLayout valueSelectionLayout;

  @JsonProperty("dataSource")
  private String dataSource;

  /**
   * Read the cohort query in a json.
   */
  public static CohortQueryLayout readFromJson(File in) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    CohortQueryLayout instance = mapper.readValue(in, CohortQueryLayout.class);
    return instance;
  }

  public static CohortQueryLayout readFromJson(String path) throws IOException {
    return readFromJson(new File(path));
  }

  /**
   * Return the schema set.
   */
  public HashSet<String> getSchemaSet() {
    HashSet<String> ret = new HashSet<>();
    ret.addAll(this.birthSelectionLayout.getRelatedSchemas());
    ret.add(this.cohortSelectionLayout.getFieldSchema());
    ret.addAll(this.valueSelectionLayout.getSchemaList());
    ret.add(this.valueSelectionLayout.getChosenSchema());
    return ret;
  }
}
