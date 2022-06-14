package com.nus.cool.core.cohort.refactor;

import java.util.Calendar;
import java.util.HashMap;

import com.google.common.base.Preconditions;

/**
 * for every user, we maintain a queue to record timewindow chosen event
 */
public class BirthSelectionContext {

  private HashMap<Integer, Calendar> userSelected; // If user is selected and BirthEvent Calender
  private HashMap<Integer, BirthContextWindow> userBirthTime; // UserId -> BirthContextWindow
  private final TimeWindow maxTimeWindow;
  private final int[] eventMinFrequency; // EventId (index) -> Frequency (value)

  public BirthSelectionContext(TimeWindow tWindow, int[] freq) {
    this.maxTimeWindow = tWindow;
    this.userSelected = new HashMap<>();
    this.userBirthTime = new HashMap<>();
    this.eventMinFrequency = freq;
  }

  /**
   * Add a event which is pass the eventSelection
   * 
   * @param userId
   * @param EventId
   * @param date
   */
  public void Add(Integer userId, Integer eventId, Calendar date) {
    Preconditions.checkArgument(userSelected.containsKey(userId) == false,
        "Before Invoke Add, the userId should be unselected state");
    if (!userBirthTime.containsKey(userId))
      this.userBirthTime.put(userId, new BirthContextWindow(maxTimeWindow, this.eventMinFrequency.length));
    // Initialize a BirthContextWindow
    BirthContextWindow contextWindow = this.userBirthTime.get(userId);
    contextWindow.put(eventId, date);

    // check whether the contextWindow meet the MinFrequency
    // if so, update the event birth date.
    if (IsSatisfied(contextWindow.getEventState())) {
      // If Satisfied, we update the birthEventTime
      // the new added event make the ContextWindow satisfy the requirement
      // means the new added event's date is the "birth Time"
      userSelected.put(userId, date);
      userBirthTime.remove(userId);
      // free the context content for selected user.
    }
  }

  /**
   * Judge whether the user's birthEvent is selected
   * 
   * @param userId
   * @return
   */
  public boolean IsUserSelected(Integer userId) {
    return userSelected.containsKey(userId);
  }

  /**
   * get the Calender of birthEvent date of certain user
   * 
   * @param userId
   * @return null if user is not selected
   */
  public Calendar getUserBirthEventDate(Integer userId) {
    return userSelected.get(userId);
  }

  /**
   * check whether in ContextWindow the birthEvent requirement is satisfied.
   * 
   * @param eventState
   * @return
   */
  private boolean IsSatisfied(int[] eventState) {
    for (int i = 0; i < eventMinFrequency.length; i++) {
      if (eventState[i] < eventMinFrequency[i]) {
        return false;
      }
    }
    return true;
  }

}
