package com.nus.cool.core.cohort.valueselect;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.nus.cool.core.cohort.aggregate.AggregateFunc;
import com.nus.cool.core.cohort.filter.Filter;
import com.nus.cool.core.cohort.storage.ProjectedTuple;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;

/**
 * Value Selection.
 */
public class ValueSelection {

  @Getter
  private List<Filter> filterList;

  @JsonIgnore
  private List<String> schemaList;
  // private

  @JsonIgnore
  @Getter
  private AggregateFunc aggregateFunc;

  /**
   * Constructor.
   */
  public ValueSelection(List<Filter> filterList,
      AggregateFunc aggregateFunc) throws IllegalArgumentException {
    Preconditions.checkArgument(aggregateFunc != null, "AggregateFunc shouldn't be null");
    if (filterList == null) {
      this.filterList = new ArrayList<>();
    } else {
      this.filterList = filterList;
    }
    this.aggregateFunc = aggregateFunc;
  }

  /**
   * The filter will outline abnormal tuples.
   *
   * @param tuple tuple
   */
  public boolean isSelected(ProjectedTuple tuple) {
    // for (Filter filter : filterList) {
    //   if (!filter.accept(tuple.getValueBySchema(filter.getFilterSchema()).getInt())) {
    //     return false;
    //   }
    // }
    // return true;
    return filterList.stream()
                     .allMatch(x -> x.accept(tuple.getValueBySchema(x.getFilterSchema()).getInt()));
  }
}
