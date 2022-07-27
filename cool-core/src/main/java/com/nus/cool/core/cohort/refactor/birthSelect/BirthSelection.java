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

@Getter
public class BirthSelection {

    // if the birthEvents is null
    // we thought that any action condition can be regareded as birthAction
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<EventSelection> birthEvents;
    
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private TimeWindow timeWindow;

    @JsonIgnore
    private BirthSelectionContext context;

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
     * 
     * @param userId
     * @return
     */
    public boolean isUserSelected(String userId) {
        return context.isUserSelected(userId);
    }

    /**
     * If user's birthEvent is selected
     * Get the BirthEvent Date to generate "Age" in Cohort
     * 
     * @param userId
     * @return
     */
    public LocalDateTime getUserBirthEventDate(String userId) {
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

    // -------------- the method is for UnitTest and Debug -------------- //
    // public static BirthSelection readFromJson(File in) throws IOException {
    //     ObjectMapper objectMapp
    // }

    /**
     * 
     * @return usersId which is eligiable for conditions
     */
    public Set<String> getAcceptedUsers(){
        return this.context.getSelectedUserId();
    }
}
