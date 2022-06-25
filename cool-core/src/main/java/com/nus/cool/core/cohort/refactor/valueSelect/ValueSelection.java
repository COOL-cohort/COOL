package com.nus.cool.core.cohort.refactor.valueSelect;

import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.nus.cool.core.cohort.refactor.aggregate.AggregateFactory;
import com.nus.cool.core.cohort.refactor.aggregate.AggregateFunc;
import com.nus.cool.core.cohort.refactor.aggregate.AggregateType;
import com.nus.cool.core.cohort.refactor.filter.Filter;
import com.nus.cool.core.cohort.refactor.filter.FilterLayout;
import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.storage.RetUnit;

import lombok.Getter;

public class ValueSelection {

    private ArrayList<FilterLayout> filters;

    @JsonIgnore
    private ArrayList<Filter> filterList;

    private String function;

    private String observedSchema;

    @JsonIgnore
    @Getter
    private ArrayList<String> schemaList;
    // private

    @JsonIgnore
    private AggregateFunc aggregateFunc;

    public void init() {
        this.filterList = new ArrayList<>();
        this.schemaList = new ArrayList<>();
        for (FilterLayout layout : this.filters) {
            this.filterList.add(layout.generateFilter());
            this.schemaList.add(layout.getFieldSchema());
        }
        this.filters = null;
        // reclaim the FilterLayout
        // create func
        this.aggregateFunc = AggregateFactory.generateAggregate(this.function);
    }

    /**
     * The filter will outline abnormal tuples
     * 
     * @param tuple
     * @return
     */
    public boolean IsSelected(ProjectedTuple tuple) {
        for (Filter filter : filterList) {
            switch (filter.getType()) {
                case Range:
                    if (!filter.accept((Integer) tuple.getValueBySchema(filter.getFilterSchema())))
                        return false;
                case Set:
                    if (!filter.accept((String) tuple.getValueBySchema(filter.getFilterSchema())))
                        return false;
                default:
                    throw new IllegalArgumentException(
                            String.format("No filter of this type named %s", filter.getType()));
            }
        }
        return true;
    }

    /**
     * change the RetUnit according the aggregatefunc and modify it in place
     * 
     * @param retUnit
     * @param tuple
     */
    public void updateRetUnit(RetUnit retUnit, ProjectedTuple tuple) {
        if (this.aggregateFunc.getType() == AggregateType.COUNT) {
            this.aggregateFunc.calulate(retUnit, 0);
        }
        // TODO(lingze)
        // add validation step, this code is dangerous
        int v = (int) (tuple.getValueBySchema(this.observedSchema));
        this.aggregateFunc.calulate(retUnit, v);
    }

}
