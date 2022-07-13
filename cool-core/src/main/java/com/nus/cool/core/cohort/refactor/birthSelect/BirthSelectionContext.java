package com.nus.cool.core.cohort.refactor.birthSelect;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.nus.cool.core.cohort.refactor.utils.DateUtils;
import com.nus.cool.core.cohort.refactor.utils.TimeWindow;

/**
 * for every user, we maintain a queue to record timewindow chosen event
 */
public class BirthSelectionContext {

  private HashMap<String, LocalDateTime> userSelected; // If user is selected and BirthEvent Calender
  private HashMap<String, BirthContextWindow> userBirthTime; // UserId -> BirthContextWindow
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
    if (IsSatisfied(contextWindow.getEventState())) {
      // If Satisfied, we update the birthEventTime
      // the new added event make the ContextWindow satisfy the requirement
      // means the new added event's date is the "birth Time"
      System.out.println("UserId " + userId + "\tdate:" + DateUtils.convertString(date));
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
  public boolean IsUserSelected(String userId) {
    return userSelected.containsKey(userId);
  }

  /**
   * get the Calender of birthEvent date of certain user
   * 
   * @param userId
   * @return null if user is not selected
   */
  public LocalDateTime getUserBirthEventDate(String userId) {
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

  // --------- only for unit test

  public Set<String> getSelectedUserId() {
    return this.userSelected.keySet();
  }

}
