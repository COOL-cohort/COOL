package com.nus.cool.core.cohort.refactor.filter;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.base.Preconditions;

import lombok.Getter;

/**
 * Read from json file into FilterLayout
 * Parse to construct certain filter class
 **/
public class FilterLayout {
    @Getter
    private String fieldSchema;

    @Getter
    private FilterType type;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Getter
    private String[] acceptValue;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Getter
    private String[] rejectValue;

    @Override
    public String toString() {
        String acStr = "", rejStr = "";
        if(this.acceptValue != null) acStr = acceptValue.toString();
        if(this.rejectValue != null) rejStr = rejectValue.toString();
        return String.format("FilterLayout fieldSchema %s filter type %s, acceptValue %s, rejectValue %s",
                fieldSchema, type, acStr, rejStr);
    }

    public Filter generateFilter() {
        switch (type) {
            case Range:
                Preconditions.checkArgument(rejectValue == null,
                        "For RangeFilter, rejectValue property is null");
                return new RangeFilter(fieldSchema, acceptValue);
            case Set:
                return new SetFilter(fieldSchema, acceptValue, rejectValue);
            default:
                throw new IllegalArgumentException(
                        String.format("No filter of this type named %s", type));
        }
    }
    /**
     * Default Constructor, JsonMapper use it
     */
    public FilterLayout(){}
    // ----------------- For Test ----------------------- //
    public FilterLayout (Boolean IsSet, String[] acList, String[] rejList){
        if (IsSet) this.type = FilterType.Set;
        else  this.type = FilterType.Range;
        this.acceptValue = acList;
        this.rejectValue = rejList;
    }
}
