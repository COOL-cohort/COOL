package com.nus.cool.core.cohort.refactor;

import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;

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
    
    public void process(ChunkRS chunk) {
        
    }

    public void process(MetaChunkRS metachunk){

    }

}
