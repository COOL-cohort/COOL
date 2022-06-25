package com.nus.cool.core.cohort.refactor.valueSelect;

import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.nus.cool.core.cohort.refactor.filter.Filter;
import com.nus.cool.core.cohort.refactor.filter.FilterLayout;
import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;

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

    public void init() {
        this.filterList = new ArrayList<>();
        this.schemaList = new ArrayList<>();
        for (FilterLayout layout : this.filters) {
            this.filterList.add(layout.generateFilter());
            this.schemaList.add(layout.getFieldSchema());
        }
        this.filters = null;
        // reclaim the FilterLayout
    }

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

}
