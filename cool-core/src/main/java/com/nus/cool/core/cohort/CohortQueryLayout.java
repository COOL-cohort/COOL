package com.nus.cool.core.cohort;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.ageselect.AgeSelectionLayout;
import com.nus.cool.core.cohort.birthselect.BirthSelectionLayout;
import com.nus.cool.core.cohort.cohortselect.CohortSelectionLayout;
import com.nus.cool.core.cohort.filter.FilterType;
import com.nus.cool.core.cohort.valueselect.ValueSelectionLayout;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import lombok.Getter;

/**
 * Layout class to facilitate json serialization of cohort query.
 */
@Getter
public class CohortQueryLayout {

  @JsonProperty("queryName")
  private String queryName;

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

  @JsonProperty("inputCohort")
  private String inputCohort;

  /**
   * Read the cohort query in a json.
   */
  public static CohortQueryLayout readFromJson(File in) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    CohortQueryLayout instance = mapper.readValue(in, CohortQueryLayout.class);
    if (instance.queryName == null) {
      throw new IOException("[x] please add a query Name for this query.");
    }
    if (instance.cohortSelectionLayout == null) {
      instance.cohortSelectionLayout = new CohortSelectionLayout();
    }
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
    if (this.birthSelectionLayout != null) {
      ret.addAll(this.birthSelectionLayout.getRelatedSchemas());
    }
    if (this.cohortSelectionLayout.getType() != FilterType.ALL) {
      ret.add(this.cohortSelectionLayout.getFieldSchema());
    }
    if (this.valueSelectionLayout != null) {
      ret.addAll(this.valueSelectionLayout.getSchemaList());
      ret.add(this.valueSelectionLayout.getChosenSchema());
    }
    return ret;
  }
}
