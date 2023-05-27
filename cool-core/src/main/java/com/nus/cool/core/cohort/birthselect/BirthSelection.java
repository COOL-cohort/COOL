package com.nus.cool.core.cohort.birthselect;

import com.nus.cool.core.cohort.storage.ProjectedTuple;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

/**
 * This class shows how to choose the birthAction according to cohort query.
 */
// @Getter
public class BirthSelection {

  // a list of events filter, if any item pass the filter, it can be considered as
  // selected event.
  // if the birthEvents is null, we thought that any action condition can be
  // regareded as birthAction
  private List<EventSelection> birthEvents;

  private BirthSelectionContext context;

  /**
   * Create BirthSelection from BirthSelectionLayout.
   *
   * @param events  list of events
   * @param context context
   */
  public BirthSelection(List<EventSelection> events, BirthSelectionContext context) {
    this.birthEvents = events;
    this.context = context;
  }

  /**
   * If user's birthEvent is selected return true else return false.
   *
   * @param userId userId
   * @return whether the user's birthEvent is selected
   */
  public boolean isUserSelected(String userId) {
    return context.isUserSelected(userId);
  }

  /**
   * Reset the selected attribute.
   *
   */
  public void removeUserSelected(String userId) {
    context.removeUserSelected(userId);
  }

  /**
   * If user's birthEvent is selected, Get the birthEvent's Date.
   * If cohort query requires a collection of event
   * we take the first event in whole collection as user's birthEvent
   *
   * @param userId userId
   * @return LocalDateTime
   */
  public LocalDateTime getUserBirthEventDate(String userId) {
    return context.getUserBirthEventDate(userId);
  }

  /**
   * Select input Action Tuple, if it can not be selected as event return false
   * Else return true and add this event into BirthSelectionContext for further
   * check.
   *
   * @param userId userId
   * @param date   actionTime
   * @param tuple  Partial Action Tuple,
   * @return true or false
   */
  public boolean selectEvent(String userId, LocalDateTime date, ProjectedTuple tuple) {
    int eventIdx = 0;
    // if no birthEvents in query
    // which means that all event is acceptable
    // thus, we just to record the first actionItem's time.
    if (birthEvents.isEmpty()) {
      if (!context.isUserSelected(userId)) {
        context.setUserSelected(userId, date);
      }
      return true;
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
   * Get all accepted users.
   *
   * @return a list of userId, in which any user have eligiable birthEvent
   */
  public Set<String> getAcceptedUsers() {
    return this.context.getSelectedUserId();
  }

  /**
   * Initialize filters with meta chunk info.
   */
  public void loadMetaInfo(MetaChunkRS metachunk) {
    for (EventSelection es : this.birthEvents) {
      es.loadMetaInfo(metachunk);
    }
  }

  /**
   * Determine if we can skip the cubelet.
   *
   * @return If none of the values in the Cublet will be accepted by the filter.
   */
  public Boolean maybeSkipMetaChunk(MetaChunkRS metachunk) {
    if (birthEvents.isEmpty()) {
      return false;
    }
    // other logics to be implemented
    return false;
  }
}
