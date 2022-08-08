package com.nus.cool.core.cohort.refactor.valueSelect;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nus.cool.core.cohort.refactor.aggregate.AggregateFactory;
import com.nus.cool.core.cohort.refactor.aggregate.AggregateFunc;
import com.nus.cool.core.cohort.refactor.aggregate.AggregateType;
import com.nus.cool.core.cohort.refactor.filter.Filter;
import com.nus.cool.core.cohort.refactor.filter.FilterLayout;

import lombok.Getter;

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

    public ValueSelection generate(){
        List<Filter> filterList = null;
        if(this.filters != null) {
            filterList = new ArrayList<>();
            for(FilterLayout layout : this.filters){
                filterList.add(layout.generateFilter());
            }
        }
        AggregateFunc func =  AggregateFactory.generateAggregate(functionType, chosenSchema);
        return new ValueSelection(filterList, func);
    }

    public List<String> getSchemaList(){
        if(schemaList == null){
            schemaList = new HashSet<>();
            if(this.filters == null) return new ArrayList<>(schemaList);
            for(FilterLayout layout: this.filters){
                schemaList.add(layout.getFieldSchema());
            }
        }
        return new ArrayList<>(schemaList);
    }
    
}
