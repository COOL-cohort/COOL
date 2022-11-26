package com.nus.cool.core.cohort.birthselect;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nus.cool.core.cohort.filter.Filter;
import com.nus.cool.core.cohort.filter.FilterLayout;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;

/**
 * EventSelection Query Layout.
 * Mapped directly from json file
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
   * Create a EventSelection instance.
   *
   * @return EventSelection
   */
  public EventSelection generate() {
    ArrayList<Filter> filterList = new ArrayList<>();
    for (FilterLayout layout : filters) {
      filterList.add(layout.generateFilter());
    }
    return new EventSelection(filterList);
  }

  /**
   * Get related schema in this event filter.
   *
   * @return List of schema
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
