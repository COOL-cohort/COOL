package com.nus.cool.core.cohort.refactor.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * enum class for filtertype
 */
public enum FilterType {
    Range("RANGE"), Set("SET");

    private final String text;

    private FilterType(final String text) {
        this.text =text;
    }

    @JsonValue
    @Override
    public String toString(){
        return text;
    }

    @JsonCreator
    public static FilterType forValue(String value){
        switch(value){
            case "RANGE": return Range;
            case "SET": return Set;
            default:
                throw new IllegalArgumentException();
        }
    }


}
