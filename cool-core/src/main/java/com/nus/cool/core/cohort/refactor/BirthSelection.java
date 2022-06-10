package com.nus.cool.core.cohort.refactor;

import java.util.ArrayList;

public class BirthSelection {

    private ArrayList<EventSelection> birthEvents;

    private TimeUtils.TimeWindow timeWindow;

    private BirthSelectionContext context;    

    public void init(){
        for(EventSelection e :birthEvents) {
            e.init();
        }
    }


}
