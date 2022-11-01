package com.nus.cool.core.cohort.refactor.birthselect;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nus.cool.core.cohort.refactor.filter.Filter;
import com.nus.cool.core.cohort.refactor.filter.FilterLayout;

import lombok.Getter;

public class EventSelectionLayout {
    @Getter
    @JsonProperty("filters")
    private List<FilterLayout> filters;

    @JsonIgnore
    private List<String> schemaList;

    @Getter
    @JsonProperty("frequency")
    private int frequency;

    public EventSelection generate() {
        ArrayList<Filter> filterList = new ArrayList<>();
        for (FilterLayout layout : filters) {
            filterList.add(layout.generateFilter());
        }
        return new EventSelection(filterList);
    };

    public List<String> getSchemaList() {
        if (this.schemaList != null)
            return this.schemaList;
        this.schemaList = new ArrayList<>();
        for (FilterLayout layout : filters) {
            this.schemaList.add(layout.getFieldSchema());
        }
        return this.schemaList;
    }
}
