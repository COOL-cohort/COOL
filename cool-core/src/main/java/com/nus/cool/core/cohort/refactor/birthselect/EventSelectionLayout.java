package com.nus.cool.core.cohort.refactor.birthselect;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nus.cool.core.cohort.refactor.filter.Filter;
import com.nus.cool.core.cohort.refactor.filter.FilterLayout;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;

/**
 * Layout class to facilitate json serialization of event selection criteria.
 */
public class EventSelectionLayout {
  @Getter
  @JsonProperty("filters")
  private List<FilterLayout> filters;

  @JsonIgnore
  private List<String> schemaList;

  @Getter
  @JsonProperty("frequency")
  private int frequency;

  /**
   * Deserialize EventSelection.
   */
  public EventSelection generate() {
    ArrayList<Filter> filterList = new ArrayList<>();
    for (FilterLayout layout : filters) {
      filterList.add(layout.generateFilter());
    }
    return new EventSelection(filterList);
  }

  /**
   * Return schema in a list.
   */
  public List<String> getSchemaList() {
    if (this.schemaList != null) {
      return this.schemaList;
    }
    this.schemaList = new ArrayList<>();
    for (FilterLayout layout : filters) {
      this.schemaList.add(layout.getFieldSchema());
    }
    return this.schemaList;
  }
}
