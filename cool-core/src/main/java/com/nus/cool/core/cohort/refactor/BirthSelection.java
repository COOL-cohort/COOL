package com.nus.cool.core.cohort.refactor;

import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.nus.cool.core.io.readstore.CubletRS;

public class BirthSelection {

    private ArrayList<EventSelection> birthEvents;

    private TimeUtils.TimeWindow timeWindow;

    @JsonIgnore
    private BirthSelectionContext context;    

    public void init(){
        for(EventSelection e :birthEvents) {
            e.init();
        }
    }
    
    public void process(CubletRS cubelet) {

    }

}
