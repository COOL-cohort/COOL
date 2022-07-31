package com.nus.cool.core.cohort.refactor.birthSelect;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.nus.cool.core.cohort.refactor.utils.TimeWindow;

/**
 * Class BirthSelectionContext is a control and manage layer for birthSelector
 * 
 * for every user, we maintain a queue (BirthContextWindow) to record chosen event's frequency in a time sliding window.
 * If the events frequency of one user's BirthContextWindow satisfied the requirement (eventMinFrequency), this user is selected.
 * and no longer needed to maintain BirthContextWindow for this user.
 */
public class BirthSelectionContext {

  // If user is selected, we store the birth date for this user
  private HashMap<String, LocalDateTime> userSelected;  

  // UserId -> BirthContextWindow, for every unselected user, we maintain a queue. It's action tuple is ordered in time series
  private HashMap<String, BirthContextWindow> userBirthTime; 
  
  // the max size of timeWindow
  private final TimeWindow maxTimeWindow;

  // Required from query, min frequency for every events in timeWindow.
  // EventId (index) -> Frequency (value)
  private final int[] eventMinFrequency; 

  public BirthSelectionContext(TimeWindow tWindow, int[] freq) {
    this.maxTimeWindow = tWindow;
    this.userSelected = new HashMap<>();
    this.userBirthTime = new HashMap<>();
    this.eventMinFrequency = freq;
  }

  /**
   * Add a event into event queue
   * 
   * get the corresponding BirthContextWindow, and push new eventId into it.
   * the BirthContextWindow will automatically adjust the inner event queue when new event is pushed
   * check whether the state of birthContextWindow satisfied the requirement.
   * if satisfied, mark the corresponding user's birthAction
   * @param userId
   * @param EventId
   * @param date
   */
  public void Add(String userId, Integer eventId, LocalDateTime date) {
    Preconditions.checkArgument(userSelected.containsKey(userId) == false,
        "Before Invoke Add, the userId should be unselected state");
    if (!userBirthTime.containsKey(userId))
      this.userBirthTime.put(userId, new BirthContextWindow(maxTimeWindow, this.eventMinFrequency.length));
    // Initialize a BirthContextWindow
    BirthContextWindow contextWindow = this.userBirthTime.get(userId);
    contextWindow.put(eventId, date);

    // check whether the contextWindow meet the MinFrequency
    // if so, update the event birth date.
    if (isSatisfied(contextWindow.getEventState())) {
      // If Satisfied, we update the birthEventTime
      // the new added event make the ContextWindow satisfy the requirement
      // means the new added event's date is the "birth Time"
      // System.out.println("UserId " + userId + "\tdate:" + DateUtils.convertString(date));
      userSelected.put(userId, date);
      userBirthTime.remove(userId);
      // free the context content for selected user.
    }
  }

  /**
   * whether the user's birthEvent is selected
   * @param userId
   * @return
   */
  public boolean isUserSelected(String userId) {
    return userSelected.containsKey(userId);
  }

  /**
   * Directly set user's Action time
   * Skip to record the frequency of events
   * (this method will invoked when the birthEvents is Null)
   * @param userId
   * @param date
   */
  public void setUserSelected(String userId, LocalDateTime date){
    if(!userSelected.containsKey(userId)){
      userSelected.put(userId, date);
    }
  }

  /**
   * get the birthEvent's datetime of certain user
   * @param userId
   * @return null if user is not selected
   */
  public LocalDateTime getUserBirthEventDate(String userId) {
    return userSelected.get(userId);
  }

  /**
   * check whether in ContextWindow the birthEvent requirement is satisfied.
   * @param eventState
   * @return
   */
  private boolean isSatisfied(int[] eventState) {
    for (int i = 0; i < eventMinFrequency.length; i++) {
      if (eventState[i] < eventMinFrequency[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Get the set of selecetd User Id
   * @return
   */
  public Set<String> getSelectedUserId() {
    return this.userSelected.keySet();
  }

}
