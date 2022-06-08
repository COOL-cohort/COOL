package com.nus.cool.core.cohort.refactor.filter;

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
        return String.format("FilterLayout fieldSchema %s filter type %s, acceptValue %s, rejectValue %s",
                fieldSchema, type, acceptValue.toString(), rejectValue.toString());
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
}
