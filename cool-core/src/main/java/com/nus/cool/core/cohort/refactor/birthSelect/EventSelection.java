package com.nus.cool.core.cohort.refactor.birthSelect;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.filter.Filter;
import com.nus.cool.core.cohort.refactor.filter.FilterLayout;
import com.nus.cool.core.cohort.refactor.filter.FilterType;

import lombok.Getter;

/**
 * EventSelection is a collection of filters
 * json mapper to constuct EventSelection
 */
public class EventSelection {
    @Getter
    private List<FilterLayout> filters;

    @JsonIgnore
    private List<Filter> filterList;

    // Help Wrapper to input ordered tuple
    @JsonIgnore
    @Getter
    private List<String> schemaList;

    @Getter
    private int frequency;
    /**
     * Create Filter from FilterLayout
     */
    public void init() {
        filterList = new ArrayList<>();
        schemaList = new ArrayList<>();
        for (FilterLayout layout : filters) {
            filterList.add(layout.generateFilter());
            schemaList.add(layout.getFieldSchema());
        }
        // Can free FilterLayout and GC will reclaim
        // This memory won't be used in future
        this.filters = null;
    }

    /**
     * 
     * @param ProjectTuple, row of one Tuple
     * @return whether this item can be chosen as a birthEvents
     */
    public boolean Accept(ProjectedTuple projectTuple) {
        for (int i = 0; i < filterList.size(); i++) {
            String schema = schemaList.get(i);
            if (filterList.get(i).getType().equals(FilterType.Set)) {
                if (!filterList.get(i).accept((String) projectTuple.getValueBySchema(schema)))
                    return false;
            } else {
                if (!filterList.get(i).accept((Integer) projectTuple.getValueBySchema(schema)))
                    return false;
            }
        }

        return true;
    }

}
