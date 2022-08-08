package com.nus.cool.core.cohort.refactor;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.refactor.ageSelect.AgeSelectionLayout;
import com.nus.cool.core.cohort.refactor.birthSelect.BirthSelectionLayout;
import com.nus.cool.core.cohort.refactor.cohortSelect.CohortSelectionLayout;
import com.nus.cool.core.cohort.refactor.valueSelect.ValueSelectionLayout;

import lombok.Getter;

@Getter
public class CohortQueryLayout {

    @JsonProperty("birthSelector")
    private BirthSelectionLayout birthSelectionLayout;


    @JsonProperty("cohortSelector")
    private CohortSelectionLayout cohortSelectionLayout;

    @JsonProperty("ageSelector")
    private AgeSelectionLayout agetSelectionLayout;

    @JsonProperty("valueSelector")
    private ValueSelectionLayout valueSelectionLayout;

    @JsonProperty("dataSource")
    private String dataSource;

    public static CohortQueryLayout readFromJson(File in) throws IOException{
        ObjectMapper mapper = new ObjectMapper();
        CohortQueryLayout instance = mapper.readValue(in, CohortQueryLayout.class);
        return instance;
    }   

    public static CohortQueryLayout readFromJson(String path) throws IOException{
        return readFromJson(new File(path));
    }

    public HashSet<String> getSchemaSet(){
        HashSet<String> ret = new HashSet<>();
        ret.addAll(this.birthSelectionLayout.getRelatedSchemas());
        ret.add(this.cohortSelectionLayout.getFieldSchema());
        ret.addAll(this.valueSelectionLayout.getSchemaList());
        ret.add(this.valueSelectionLayout.getChosenSchema());
        return ret;
    }

}
