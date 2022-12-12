package com.nus.cool.core.cohort.valueselect;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nus.cool.core.cohort.aggregate.AggregateFactory;
import com.nus.cool.core.cohort.aggregate.AggregateFunc;
import com.nus.cool.core.cohort.aggregate.AggregateType;
import com.nus.cool.core.cohort.filter.Filter;
import com.nus.cool.core.cohort.filter.FilterLayout;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import lombok.Getter;

/**
 * ValueSelection query layout.
 * mapped directly from json file
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
   * Constructor.
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
   * Get related schema list.
   *
   * @return list of schema
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
