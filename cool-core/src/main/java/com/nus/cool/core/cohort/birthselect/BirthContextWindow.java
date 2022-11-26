package com.nus.cool.core.cohort.birthselect;

import com.google.common.base.Preconditions;
import com.nus.cool.core.cohort.utils.DateUtils;
import com.nus.cool.core.cohort.utils.TimeWindow;
import java.time.LocalDateTime;
import java.util.LinkedList;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * BirthContextWindow is a sliding window based on adaptive queue.
 * It keep a maxTimeWindow and a time-based queue (LinkedList)
 * When new (eventId, datetime) input, this queue will automatically pop
 * elements
 * to keep the time duration between the first and the last element is less than
 * the maxTimeWindow
 */
public class BirthContextWindow {

  // store EventTime in a queue, in time order
  private LinkedList<EventTime> window;

  // store the frequency of chosen Event in this Window
  @Getter
  private int[] eventState;

  private final int eventNum;

  // the max timeWindow
  private final TimeWindow maxTimeWindow;

  /**
   * Constructor.
   *
   * @param tWindow  timeWindow setting
   * @param eventNum the quantity of all total event
   */
  public BirthContextWindow(TimeWindow tWindow, int eventNum) {
    this.maxTimeWindow = tWindow;
    this.eventState = new int[eventNum];
    this.eventNum = eventNum;
    this.window = new LinkedList<>();
  }

  /**
   * Put new Chosen EventId and corresponding time into queue.
   *
   * @param eventId the index of event
   * @param date    date
   */
  public void put(Integer eventId, LocalDateTime date) {
    Preconditions.checkState(eventId < this.eventNum,
        "Input eventId is out of range");
    if (this.maxTimeWindow == null) {
      // no need to maintain window, all action tuple should be considered
      // consider the window is infinete max
      this.eventState[eventId] += 1;
      return;
    }
    EventTime newEventTime = new EventTime(eventId, date);
    window.add(newEventTime);
    this.eventState[eventId] += 1;
    while (true) {
      EventTime peak = window.peek();
      TimeWindow duration = DateUtils.getDifference(peak.getDatetime(), newEventTime.getDatetime(),
          maxTimeWindow.getUnit());
      if (duration.compareTo(maxTimeWindow) > 0) {
        // pop out this Event
        this.pop();
      } else {
        break;
      }
    }
  }

  /**
   * Pop out the first element in queue and update the EventState.
   *
   */
  private void pop() {
    if (window.isEmpty()) {
      return;
    }
    EventTime peak = window.peek();
    window.remove();
    // update eventState , default HashMap contain peak.key
    this.eventState[peak.getEventId()] -= 1;
  }

  /**
   * Typedef a Pair(EventId, DateTime).
   */
  @Getter
  @AllArgsConstructor
  public class EventTime {
    private Integer eventId;
    private LocalDateTime datetime;
  }
}