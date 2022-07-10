package com.nus.cool.core.cohort.refactor.birthSelect;

import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.LinkedList;

import com.google.common.base.Preconditions;
import com.nus.cool.core.cohort.refactor.utils.DateUtils;
import com.nus.cool.core.cohort.refactor.utils.TimeWindow;

import javafx.util.Pair;
import lombok.Getter;

public class BirthContextWindow {
    private LinkedList<EventTime> window; // store EventTime in a queue, in time order

    @Getter
    private int[] eventState; // store the frequency of chosen Event in this Window
    private final int eventNum;
    private final TimeWindow maxTimeWindow;

    public BirthContextWindow(TimeWindow tWindow, int eventNum) {
        this.maxTimeWindow = tWindow;
        this.eventState = new int[eventNum];
        this.eventNum = eventNum;
        this.window = new LinkedList<>();
    }

    /**
     * Put new Chosen Event and TimeInto Queue
     * 
     * @param newEventTime
     */
    public void put(Integer eventId, LocalDateTime date) {
        Preconditions.checkState(eventId < this.eventNum,
                "Input eventId is out of range");
        
        if(this.maxTimeWindow == null){
            // no need to maintain window, all action tuple should be considered
            this.eventState[eventId] += 1;
            return;
        }
        EventTime newEventTime = new EventTime(eventId, date);
        window.add(newEventTime);
        this.eventState[eventId] += 1;

        while (true) {
            EventTime peak = window.peek();
            TimeWindow duration = DateUtils.getDifference(peak.getTimeCalendar(), newEventTime.getTimeCalendar(),
                    maxTimeWindow.getUnit());
            if (duration.compareTo(maxTimeWindow) > 0) {
                // pop out this Event
                this.pop();
            } else
                break;
        }

    }

    /**
     * Pop out the first element in queue and update the EventState
     */
    public void pop() {
        if (window.isEmpty())
            return;
        EventTime peak = window.peek();
        window.remove();
        // update eventState , default HashMap contain peak.key
        this.eventState[peak.getEventId()] -= 1;
    }

    /**
     * Typedef a Pair<EventId, DateTime>
     */
    private class EventTime extends Pair<Integer, LocalDateTime> {
        public EventTime(Integer EventId, LocalDateTime Date) {
            super(EventId, Date);
        }

        public Integer getEventId() {
            return super.getKey();
        }

        public LocalDateTime getTimeCalendar() {
            return super.getValue();
        }
    }
}