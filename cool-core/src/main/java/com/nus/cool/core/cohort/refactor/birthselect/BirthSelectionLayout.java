package com.nus.cool.core.cohort.refactor.birthselect;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nus.cool.core.cohort.refactor.utils.TimeWindow;

import lombok.Getter;

@Getter
public class BirthSelectionLayout {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("birthEvents")
    private List<EventSelectionLayout> eventLayoutList;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private TimeWindow timeWindow;

    @JsonIgnore
    private HashSet<String> relatedSchemas;

    public BirthSelection generate(){
        ArrayList<EventSelection> birthEvents = new ArrayList<>();
        this.relatedSchemas = new HashSet<>();
        if(this.eventLayoutList == null){
            // no birthEvent filter, means that all event can be selected as birthEvent
            return new BirthSelection(null, new BirthSelectionContext(timeWindow, null));
        }
        // init event
        int[] eventMinFrequency = new int [this.eventLayoutList.size()];
        int i = 0;
        for(EventSelectionLayout layout: this.eventLayoutList){
            birthEvents.add(layout.generate());
            this.relatedSchemas.addAll(layout.getSchemaList());
            eventMinFrequency[i++] = layout.getFrequency();
        }
        return new BirthSelection(birthEvents, new BirthSelectionContext(timeWindow, eventMinFrequency));
    } 
}
