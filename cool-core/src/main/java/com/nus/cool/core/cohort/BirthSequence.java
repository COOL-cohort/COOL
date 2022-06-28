/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.nus.cool.core.cohort;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.ArrayList;
import java.util.List;

public class BirthSequence {

    public static class CohortField {

        private String field;

        private int numLevel = 10;

        private int minLevel = 0;

        private boolean logScale = false;

        private double scale = 1.0;
 
        /**
         * @return the field
         */
        public String getField() {
            return field;
        }

        /**
         * @param field the field to set
         */
        // public void setField(String field) { this.field = field; }

        /**
         * @return the numLevel
         */
        public int getNumLevel() {
            return numLevel;
        }

        /**
         * @param numLevel the numLevel to set
         */
        // public void setNumLevel(int numLevel) { this.numLevel = numLevel; }

        /**
         * @return the minLevel
         */
        public int getMinLevel() {
            return minLevel;
        }

        /**
         * @param minLevel the minLevel to set
         */
        // public void setMinLevel(int minLevel) { this.minLevel = minLevel; }

        /**
         * @return the logScale
         */
        public boolean isLogScale() {
            return logScale;
        }

        /**
         * @param logScale the logScale to set
         */
        // public void setLogScale(boolean logScale) { this.logScale = logScale; }

        /**
         * @return the scale
         */
        public double getScale() {
            return scale;
        }

        /**
         * @param scale the scale to set
         */
        // public void setScale(double scale) { this.scale = scale; }

        @JsonIgnore
        public boolean isValid() {
            return this.scale > 0 &&
                (this.scale > 1 || !this.logScale);
        }

    }

    public static class Anchor {

        private int anchor;

        private String offset;

        @JsonIgnore
        private int low = 0;
        
        @JsonIgnore 
        private int high = 0;

        /**
         * @return the anchor
         */
        public int getAnchor() {
            return anchor;
        }

        /**
         * @param anchor the anchor to set
         */
        // public void setAnchor(int anchor) { this.anchor = anchor; }

        /**
         * @return the offsets
         */
        // public String getOffset() { return offset; }

        /**
         * @param offset the offsets to set
         */
        // public void setOffset(String offset) {
        //     this.offset = offset;
        //     String[] range = offset.split("\\|");
        //     this.low = Integer.parseInt(range[0]);
        //     this.high = Integer.parseInt(range[1]);
        // }

        @JsonIgnore
        public int getLowOffset() {
            return this.low;
        }

        @JsonIgnore
        public int getHighOffset() {
            return this.high;
        }
    }

    public static enum TimeWindowUnit {
        DAY,
        WEEK,
        MONTH
    }

    public static class TimeWindow {

        private int length;

        private boolean slice;

        private TimeWindowUnit unit = TimeWindowUnit.DAY;

        private List<Anchor> anchors = new ArrayList<>();

        /**
         * @return the length
         */
        public int getLength() {
            return length;
        }

        /**
         * @param length the duration to set
         */
        // public void setLength(int length) { this.length = length; }

        /**
         * @return the slice
         */
        public boolean getSlice() {
            return slice;
        }

        /**
         * @param slice the slice to set
         */
        // public void setSlice(boolean slice) { this.slice = slice; }

        /**
         * @return the unit
         */
        public TimeWindowUnit getUnit() { return unit; }

        /**
         * @param unit the unit to set
         */
        // public void setUnit(TimeWindowUnit unit) { this.unit = unit; }

        /**
         * @return the anchors
         */
        public List<Anchor> getAnchors() {
            return anchors;
        }

        /**
         * @param anchors the anchors to set
         */
        // public void setAnchors(List<Anchor> anchors) { this.anchors = anchors; }
    }

    public static class BirthEvent {
        private int minTrigger = 0;
        private int maxTrigger = -1;
        private List<ExtendedFieldSet> eventSelection = new ArrayList<>();
        private List<ExtendedFieldSet> aggrSelection = new ArrayList<>();
        private TimeWindow timeWindow = null;
        private List<CohortField> cohortFields = new ArrayList<>();

        public List<ExtendedFieldSet> getAggrSelection() {
            return this.aggrSelection;
        }

        // public void setAggrSelection(List<ExtendedFieldSet> selection) {
        //     this.aggrSelection= selection;
        // }

        /**
         * @return the timeWindow
         */
        public TimeWindow getTimeWindow() {
            return timeWindow;
        }

        /**
         * @param timeWindow the timeWindow to set
         */
        //public void setTimeWindow(TimeWindow timeWindow) { this.timeWindow = timeWindow; }

        /**
         * @return the cohortFields
         */
        public List<CohortField> getCohortFields() {
            return cohortFields;
        }

        /**
         * @param cohortFields the cohortFields to set
         */
        // public void setCohortFields(List<CohortField> cohortFields) { this.cohortFields = cohortFields; }

        public List<ExtendedFieldSet> getEventSelection() {
            return eventSelection;
        }

        // public void setEventSelection(List<ExtendedFieldSet> selection) { this.eventSelection = selection; }

        public int getMinTrigger() {
            return minTrigger;
        }

        //public void setMinTrigger(int num) { minTrigger = num; }

        public int getMaxTrigger() {
            return maxTrigger;
        }

        //public void setMaxTrigger(int num) { maxTrigger = num; }
    }

    private List<BirthEvent> birthEvents = new ArrayList<>();

    @JsonIgnore
    List<Integer> sortedEvents = new ArrayList<Integer>();

    public List<BirthEvent> getBirthEvents() {
        return birthEvents;
    }

    public void setBirthEvents(List<BirthEvent> list) {
        birthEvents = list;

        //topologically sort according to anchored order of time window 
        List<List<Integer>> graph = new ArrayList<List<Integer>>(birthEvents.size());
        List<Integer> events = new ArrayList<Integer>(birthEvents.size());

        for (int i = 0; i < birthEvents.size(); i++) {
            graph.add(new ArrayList<Integer>(birthEvents.size()));
            events.add(i);
        }

        for (int i = 0; i < birthEvents.size(); i++) {
            BirthEvent e = birthEvents.get(i);

            TimeWindow window = e.getTimeWindow();
            if (window != null) {
                for (Anchor anchor : window.getAnchors()) {
                    if (i != anchor.getAnchor())
                        graph.get(i).add(anchor.getAnchor()); 
                }
            }
        }

        while (sortedEvents.size() < birthEvents.size()) {
            int ev = 0;
            int event;
            while (ev < events.size() && graph.get(events.get(ev)).size() > 0)
                ev++;

            if (ev == events.size()) {
                // cycle detected
                sortedEvents.clear();
                return;
            }

            event = events.get(ev);

            events.remove(ev);

            sortedEvents.add(event);

            for (List<Integer> inEdge : graph) {
                for (int i = 0; i < inEdge.size(); i++) {
                    if (inEdge.get(i) == event) {
                        inEdge.remove(i);
                        break;
                    }
                }
            }
        }
    }


    @JsonIgnore
    public List<Integer> getSortedBirthEvents() {
        return this.sortedEvents;
    }

    @JsonIgnore
    public boolean isValid() {
        return this.birthEvents.size() == this.sortedEvents.size();
    }
}
