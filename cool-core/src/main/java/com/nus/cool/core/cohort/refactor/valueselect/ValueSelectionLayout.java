package com.nus.cool.core.cohort.refactor.valueselect;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nus.cool.core.cohort.refactor.aggregate.AggregateFactory;
import com.nus.cool.core.cohort.refactor.aggregate.AggregateFunc;
import com.nus.cool.core.cohort.refactor.aggregate.AggregateType;
import com.nus.cool.core.cohort.refactor.filter.Filter;
import com.nus.cool.core.cohort.refactor.filter.FilterLayout;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import lombok.Getter;

/**
 * Layout class to facilitate json serialization of value selection criteria.
 */
@Getter
public class ValueSelectionLayout {

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private List<FilterLayout> filters;

  @JsonProperty("function")
  private AggregateType functionType;

  @JsonProperty("observedSchema")
  private String chosenSchema;

  @JsonIgnore
  private HashSet<String> schemaList;

  /**
   * Deserialize ValueSelection.
   */
  public ValueSelection generate() {
    List<Filter> filterList = null;
    if (this.filters != null) {
      filterList = new ArrayList<>();
      for (FilterLayout layout : this.filters) {
        filterList.add(layout.generateFilter());
      }
    }
    AggregateFunc func = AggregateFactory.generateAggregate(functionType, chosenSchema);
    return new ValueSelection(filterList, func);
  }

  /**
   * Return the schema list in a list of string.
   */
  public List<String> getSchemaList() {
    if (schemaList == null) {
      schemaList = new HashSet<>();
      if (this.filters == null) {
        return new ArrayList<>(schemaList);
      }
      for (FilterLayout layout : this.filters) {
        schemaList.add(layout.getFieldSchema());
      }
    }
    return new ArrayList<>(schemaList);
  }

}
