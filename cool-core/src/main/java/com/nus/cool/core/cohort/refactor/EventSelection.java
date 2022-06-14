package com.nus.cool.core.cohort.refactor;

import java.util.ArrayList;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;

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
    public boolean Accept(Object[] ProjectTuple) {
        Preconditions.checkArgument(ProjectTuple.length == schemaList.size(),
                "Input ProjectedCol's size is equal to schemaList size");
        for (int i = 0; i < ProjectTuple.length; i++) {
            if (filterList.get(i).getType() == FilterType.Set) {
                if (!filterList.get(i).accept((String) ProjectTuple[i]))
                    return false;
            } else {
                if (!filterList.get(i).accept((Integer) ProjectTuple[i]))
                    return false;
            }
        }
        return true;
    }

}
