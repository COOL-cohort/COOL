package com.nus.cool.core.cohort.refactor.valueSelect;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.nus.cool.core.cohort.refactor.aggregate.AggregateFunc;
import com.nus.cool.core.cohort.refactor.filter.Filter;
import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.storage.RetUnit;
import lombok.Getter;

public class ValueSelection {

    @Getter
    private List<Filter> filterList;

    @JsonIgnore
    private List<String> schemaList;
    // private

    @JsonIgnore
    @Getter
    private AggregateFunc aggregateFunc;

    public ValueSelection(List<Filter> filterList, AggregateFunc aggregateFunc) throws IllegalArgumentException {
        Preconditions.checkArgument(aggregateFunc != null, "AggregateFunc shouldn't be null");
        if (filterList == null)
            this.filterList = new ArrayList<>();
        else
            this.filterList = filterList;
        this.aggregateFunc = aggregateFunc;
    }

    /**
     * The filter will outline abnormal tuples
     * @param tuple
     * @return
     */
    public boolean IsSelected(ProjectedTuple tuple) {
        for (Filter filter : filterList) {
            if (!filter.accept((Integer) tuple.getValueBySchema(filter.getFilterSchema())))
                return false;
        }return true;
    }
}
