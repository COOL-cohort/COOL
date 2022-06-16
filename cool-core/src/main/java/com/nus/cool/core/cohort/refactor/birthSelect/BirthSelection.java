package com.nus.cool.core.cohort.refactor.birthSelect;

import java.util.ArrayList;
import java.util.Calendar;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.utils.TimeWindow;

public class BirthSelection {

    private ArrayList<EventSelection> birthEvents;

    private TimeWindow timeWindow;

    @JsonIgnore
    private BirthSelectionContext context;

    @JsonIgnore
    private ArrayList<String> schemaList;

    /**
     * initialize a instance of BirthSelection load from json
     * transfer filter layout to filter instance.
     */
    public void init() {
        int[] eventMinFrequency = new int[birthEvents.size()];
        for (int i = 0; i < birthEvents.size(); i++) {
            birthEvents.get(i).init();
            eventMinFrequency[i] = birthEvents.get(i).getFrequency();
            this.schemaList.addAll(birthEvents.get(i).getSchemaList());
        }
        this.context = new BirthSelectionContext(timeWindow, eventMinFrequency);
    }

    /**
     * If user's birthEvent is selected return true else return false
     * 
     * @param userId
     * @return
     */
    public boolean isUserSelected(int userId) {
        return context.IsUserSelected(userId);
    }

    /**
     * If user's birthEvent is selected
     * Get the BirthEvent Date to generate "Age" in Cohort
     * 
     * @param userId
     * @return
     */
    public Calendar getUserBirthEventDate(int userId) {
        return context.getUserBirthEventDate(userId);
    }

    /**
     * Select input Action Tuple, if it can be selected as event return true else
     * return false
     * @param userId
     * @param date
     * @param tuple  Partial Action Tuple,
     * @return
     */
    public boolean selectEvent(int userId, Calendar date, ProjectedTuple tuple) {
        int eventIdx = 0;
        for (EventSelection event : birthEvents) {
            if (event.Accept(tuple)) {
                context.Add(userId, eventIdx, date);
                return true;
            }
            eventIdx++;
        }
        return false;
    }
}
