package com.nus.cool.core.cohort.refactor.birthSelect;

import java.util.ArrayList;
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

    private ArrayList<FilterLayout> filters;

    @JsonIgnore
    private ArrayList<Filter> filterList;

    // Help Wrapper to input ordered tuple
    @JsonIgnore
    @Getter
    private ArrayList<String> schemaList;

    @Getter
    private int frequency;
    /**
     * Create Filter from FilterLayout
     */
    public void init() {
        for (FilterLayout layout : filters) {
            filterList.add(layout.generateFilter());
            schemaList.add(layout.getFieldSchema());
        }
        // Can free FilterLayout and GC will reclaim
        // This memory won't be used in future
        filters = null;
    }

    /**
     * 
     * @param ProjectTuple, row of one Tuple
     * @return whether this item can be chosen as a birthEvents
     */
    public boolean Accept(ProjectedTuple projectTuple) {
        for (int i = 0; i < filterList.size(); i++) {
            String schema = schemaList.get(i);
            if (filterList.get(i).getType() == FilterType.Set) {
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
