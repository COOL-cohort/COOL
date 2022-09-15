package com.nus.cool.core.cohort.refactor.valueselect;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.nus.cool.core.cohort.refactor.aggregate.AggregateFunc;
import com.nus.cool.core.cohort.refactor.filter.Filter;
import com.nus.cool.core.cohort.refactor.filter.FilterType;
import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.storage.RetUnit;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;

/**
 * value selection criteria of a query.
 */
public class ValueSelection {

  @Getter
  private List<Filter> filterList;

  @JsonIgnore
  private List<String> schemaList;
  // private

  @JsonIgnore
  private AggregateFunc aggregateFunc;

  /**
   * Create a value selection that contains a list of filter and an aggregator.
   */
  public ValueSelection(List<Filter> filterList, AggregateFunc aggregateFunc)
      throws IllegalArgumentException {
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
   */
  public boolean isSelected(ProjectedTuple tuple) {
    for (Filter filter : filterList) {
      FilterType type = filter.getType();
      switch (type) {
        case Range:
          if (!filter.accept((Integer) tuple.getValueBySchema(filter.getFilterSchema()))) {
            return false;
          }
          break;
        case Set:
          if (!filter.accept((String) tuple.getValueBySchema(filter.getFilterSchema()))) {
            return false;
          }
          break;
        default:
          throw new IllegalArgumentException(
              String.format("No filter of this type named %s", filter.getType()));
      }
    }
    return true;
  }

  /**
   * Change the RetUnit according the aggregatefunc and modify it in place.
   */
  public void updateRetUnit(RetUnit retUnit, ProjectedTuple tuple) {
    this.aggregateFunc.calculate(retUnit, tuple);
  }
}
