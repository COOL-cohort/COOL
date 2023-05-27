package com.nus.cool.core.cohort.birthselect;

import com.google.common.base.Preconditions;
import com.nus.cool.core.cohort.utils.TimeWindow;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Set;

/**
 * Class BirthSelectionContext is a control and manage layer for birthSelector
 *
 * <p>
 * for every user, we maintain a queue (BirthContextWindow) to record chosen
 * event's frequency in a time sliding window. If the events frequency of one
 * user's BirthContextWindow satisfied the requirement (eventMinFrequency), this
 * user is selected. and no longer needed to maintain BirthContextWindow for
 * this user.
 */
public class BirthSelectionContext {

  // If user is selected, we store the birth date for this user
  private HashMap<String, LocalDateTime> userSelected;

  // UserId -> BirthContextWindow, for every unselected user, we maintain a queue.
  // It's action tuple is ordered in time series
  private HashMap<String, BirthContextWindow> userBirthTime;

  // the max size of timeWindow
  private final TimeWindow maxTimeWindow;

  // Required from query, min frequency for every events in timeWindow.
  // EventId (index) -> Frequency (value)
  private final int[] eventMinFrequency;

  /**
   * Create a BirthSelectionContext.
   */
  public BirthSelectionContext(TimeWindow maxTimeWindow, int[] freq) {
    this.maxTimeWindow = maxTimeWindow;
    this.userSelected = new HashMap<>();
    this.userBirthTime = new HashMap<>();
    this.eventMinFrequency = freq;
  }

  /**
   * Add a event into event queue
   *
   * <p>
   * get the corresponding BirthContextWindow, and push new eventId into it. the
   * BirthContextWindow will automatically adjust the inner event queue when new
   * event is pushed check whether the state of birthContextWindow satisfied the
   * requirement. if satisfied, mark the corresponding user's birthAction
   */
  public void add(String userId, Integer eventId, LocalDateTime date) {
    Preconditions.checkArgument(userSelected.containsKey(userId) == false,
        "Before Invoke Add, the userId should be unselected state");
    if (!userBirthTime.containsKey(userId)) {
      this.userBirthTime.put(userId,
          new BirthContextWindow(maxTimeWindow, this.eventMinFrequency.length));
    }
    // Initialize a BirthContextWindow
    BirthContextWindow contextWindow = this.userBirthTime.get(userId);
    contextWindow.put(eventId, date);
    // check whether the contextWindow meet the MinFrequency
    // if so, update the event birth date.
    if (isSatisfied(contextWindow.getEventState())) {
      // If Satisfied, we update the birthEventTime
      // the new added event make the ContextWindow satisfy the requirement
      // means the new added event's date is the "birth Time"
      userSelected.put(userId, date);
      userBirthTime.remove(userId);
      // free the context content for selected user.
    }
  }

  /**
   * whether the user's birthEvent is selected.
   */
  public boolean isUserSelected(String userId) {
    return userSelected.containsKey(userId);
  }

  /**
   * reset user's birthEvent.
   */
  public void removeUserSelected(String userId) {
    userSelected.remove(userId);
  }

  /**
   * Directly set user's Action time Skip to record the frequency of events (this
   * method will invoked when the birthEvents is Null).
   */
  public void setUserSelected(String userId, LocalDateTime date) {
    if (!userSelected.containsKey(userId)) {
      userSelected.put(userId, date);
    }
  }

  /**
   * Get the birthEvent's datetime of certain user.
   *
   * @return null if user is not selected
   */
  public LocalDateTime getUserBirthEventDate(String userId) {
    return userSelected.get(userId);
  }

  /**
   * Check whether in ContextWindow the birthEvent requirement is satisfied.
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
   * Get the set of selecetd User Id.
   */
  public Set<String> getSelectedUserId() {
    return this.userSelected.keySet();
  }
}
