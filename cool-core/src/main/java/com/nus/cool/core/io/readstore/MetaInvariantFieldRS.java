package com.nus.cool.core.io.readstore;

import com.google.common.collect.Maps;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MetaInvariantFieldRS {
    @Getter
    private Map<String, List<Object>> metaInvariantFields = Maps.newHashMap();

    @Getter
    private Map<String, Integer> invariantName2Id = Maps.newHashMap();

    @Getter
    private List<Integer> invariantKeyField = new ArrayList<Integer>();

    public MetaInvariantFieldRS(Map<String, Integer> invariantName2Id, List<Integer> invariantKeyField) {
        this.invariantName2Id=invariantName2Id;
        this.invariantKeyField=invariantKeyField;
    }
    public void addMetaInvariantField(String UserID, List<Object> invariantData){
        this.metaInvariantFields.put(UserID,invariantData);
    }
}
