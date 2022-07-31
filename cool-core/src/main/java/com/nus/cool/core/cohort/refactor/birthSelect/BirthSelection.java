package com.nus.cool.core.cohort.refactor.birthSelect;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.utils.TimeWindow;

import lombok.Getter;

/**
 * This class shows how to choose the birthAction according to cohort query
 */ 
@Getter
public class BirthSelection {

    // a list of events filter, if any item pass the filter, it can be considered as selected event. 
    // if the birthEvents is null, we thought that any action condition can be regareded as birthAction
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<EventSelection> birthEvents;
    
    // the size of time sliding window
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private TimeWindow timeWindow;

    @JsonIgnore
    private BirthSelectionContext context;

    // a set of schema used in BirthSelction part
    @JsonIgnore
    @Getter
    private HashSet<String> relatedSchemas;


    /**
     * initialize a instance of BirthSelection load from json
     * transfer filter layout to filter instance.
     */
    public void init() {
        int[] eventMinFrequency = null;
        this.relatedSchemas = new HashSet<>();
        if(birthEvents != null) {
            eventMinFrequency = new int[birthEvents.size()];
            for (int i = 0; i < birthEvents.size(); i++) {
                birthEvents.get(i).init();
                eventMinFrequency[i] = birthEvents.get(i).getFrequency();
                this.relatedSchemas.addAll(birthEvents.get(i).getSchemaList());
            }
        }
        // if birthEvents == null, there is no Null
        this.context = new BirthSelectionContext(this.timeWindow, eventMinFrequency);
    }


    /**
     * If user's birthEvent is selected return true else return false
     * @param userId 
     * @return whether the user's birthEvent is selected
     */
    public boolean isUserSelected(String userId) {
        return context.isUserSelected(userId);
    }

    /**
     * If user's birthEvent is selected, Get the birthEvent's Date.
     * If cohort query requires a collection of event
     * we take the first event in whole collection as user's birthEvent
     * 
     * @param userId
     * @return
     */
    public LocalDateTime getUserBirthEventDate(String userId) {
        return context.getUserBirthEventDate(userId);
    }

    /**
     * Select input Action Tuple, if it can not be selected as event return false
     * Else return true and add this event into BirthSelectionContext for further check.
     * @param userId
     * @param date
     * @param tuple  Partial Action Tuple,
     * @return
     */
    public boolean selectEvent(String userId, LocalDateTime date, ProjectedTuple tuple) {
        int eventIdx = 0;
        // if no birthEvents in query
        // which means that all event is acceptable
        // thus, we just to record the first actionItem's time.
        if(birthEvents == null){
            if(!context.isUserSelected(userId)){
                context.setUserSelected(userId, date);
                return true;
            }
            return false;
        }

        // if birthEvents is not null
        for (EventSelection event : birthEvents) {
            if (event.Accept(tuple)) {
                context.Add(userId, eventIdx, date);
                return true;
            }
            eventIdx++;
        }
        return false;
    }

    /** 
     * @return a list of userId, in which any user have eligiable birthEvent
     */
    public Set<String> getAcceptedUsers(){
        return this.context.getSelectedUserId();
    }
}
