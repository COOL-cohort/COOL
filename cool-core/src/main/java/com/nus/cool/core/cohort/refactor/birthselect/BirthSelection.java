package com.nus.cool.core.cohort.refactor.birthselect;

import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import lombok.Getter;

/**
 * This class shows how to choose the birthAction according to cohort query.
 */
@Getter
public class BirthSelection {

  // a list of events filter, if any item pass the filter, it can be considered as
  // selected event.
  // if the birthEvents is null, we thought that any action condition can be
  // regareded as birthAction
  private List<EventSelection> birthEvents;

  private BirthSelectionContext context;

  /**
   * Create BirthSelection from BirthSelectionLayout.
   */
  public BirthSelection(List<EventSelection> events, BirthSelectionContext context) {
    this.birthEvents = events;
    this.context = context;
  }

  /**
   * If user's birthEvent is selected return true else return false.

   * @return whether the user's birthEvent is selected
   */
  public boolean isUserSelected(String userId) {
    return context.isUserSelected(userId);
  }

  /**
   * If user's birthEvent is selected, Get the birthEvent's Date. If cohort query
   * requires a collection of event we take the first event in whole collection as
   * user's birthEvent
   */
  public LocalDateTime getUserBirthEventDate(String userId) {
    return context.getUserBirthEventDate(userId);
  }

  /**
   * Select input Action Tuple, if it can not be selected as event return false
   * Else return true and add this event into BirthSelectionContext for further
   * check.

   * @param tuple  Partial Action Tuple,
   */
  public boolean selectEvent(String userId, LocalDateTime date, ProjectedTuple tuple) {
    int eventIdx = 0;
    // if no birthEvents in query
    // which means that all event is acceptable
    // thus, we just to record the first actionItem's time.
    if (birthEvents == null) {
      if (!context.isUserSelected(userId)) {
        context.setUserSelected(userId, date);
        return true;
      }
      return false;
    }

    // if birthEvents is not null
    for (EventSelection event : birthEvents) {
      if (event.accept(tuple)) {
        context.add(userId, eventIdx, date);
        return true;
      }
      eventIdx++;
    }
    return false;
  }

  /**
   * Returns the list of userId of users having eligiable birthEvent.
   */
  public Set<String> getAcceptedUsers() {
    return this.context.getSelectedUserId();
  }
}
