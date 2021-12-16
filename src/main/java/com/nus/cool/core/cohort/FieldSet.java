package com.nus.cool.core.cohort;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class FieldSet {

    private FieldSetType fieldSetType;

    private String field;

    private List<String> values;

    public enum FieldSetType {

        Set,

        Range
    }
}
