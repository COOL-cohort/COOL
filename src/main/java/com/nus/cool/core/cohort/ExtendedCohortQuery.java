package com.nus.cool.core.cohort;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.Data;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

@Data
public class ExtendedCohortQuery {

    @Data
    public static class AgeField {

        private String field;

        private TimeUnit unit = TimeUnit.DAY;

        private int ageInterval = 1;

        private List<ExtendedFieldSet> eventSelection = new ArrayList<>();

        private List<String> range;

        private boolean fillWithLastObserved = false;

        private boolean fillWithNextObserved = false;

//        /**
//         * @return the field
//         */
//        public String getField() {
//            return field;
//        }
//
//        /**
//         * @param field the field to set
//         */
//        public void setField(String field) {
//            this.field = field;
//        }
//
//        /**
//         * @return the ageInterval
//         */
//        public int getAgeInterval() {
//            return ageInterval;
//        }
//
//        /**
//         * @param ageInterval the ageInterval to set
//         */
//        public void setAgeInterval(int ageInterval) {
//            this.ageInterval = ageInterval;
//        }
//
//        /**
//         * @return the ageSelection
//         */
//        public List<ExtendedFieldSet> getEventSelection() {
//            return eventSelection;
//        }
//
//        /**
//         * @param eventSelection the eventSelection to set
//         */
//        public void setEventSelection(List<ExtendedFieldSet> eventSelection) {
//            this.eventSelection = eventSelection;
//        }
//
//        /**
//         * @return the range
//         */
//        public List<String> getRange() {
//            return range;
//        }
//
//        /**
//         * @param range the range to set
//         */
//        public void setRange(List<String> range) {
//            this.range = range;
//        }

//        public TimeUnit getUnit() {
//            return unit;
//        }
//
//        public void setUnit(TimeUnit unit) {
//            this.unit = unit;
//        }
//
//        public boolean isFillWithLastObserved() {
//            return fillWithLastObserved;
//        }
//
//        public void setFillWithLastObserved(boolean fillWithLastObserved) {
//            this.fillWithLastObserved = fillWithLastObserved;
//        }

        public boolean isFillWithNextObserved() {
            return fillWithNextObserved;
        }

        public void setFillWithNextObserved(boolean fillWithNextObserved) {
            this.fillWithNextObserved = fillWithNextObserved;
        }
    }

    private static final Log LOG = LogFactory.getLog(ExtendedCohortQuery.class);

    private String dataSource;

    private String appKey;

    private BirthSequence birthSequence = new BirthSequence();

    private AgeField ageField = null;

    private List<ExtendedFieldSet> ageSelection = null;

    private String measure;

    private String inputCohort;

    private String outputCohort;

    private String userId;

//    /**
//     * @return the dataSource
//     */
//    public String getDataSource() {
//        return dataSource;
//    }
//
//    /**
//     * @param dataSource the dataSource to set
//     */
//    public void setDataSource(String dataSource) {
//        this.dataSource = dataSource;
//    }
//
//    /**
//     * @return the appKey
//     */
//    public String getAppKey() {
//        return appKey;
//    }
//
//    /**
//     * @param appKey the appKey to set
//     */
//    public void setAppKey(String appKey) {
//        this.appKey = appKey;
//    }
//
//    /**
//     * @return the birthSequence
//     */
//    public BirthSequence getBirthSequence() {
//        return birthSequence;
//    }
//
//    /**
//     * @param seq the BirthSequence to set
//     */
//    public void setBirthSequence(BirthSequence seq) {
//        birthSequence = seq;
//    }
//
//    /**
//     * @return the ageField
//     */
//    public AgeField getAgeField() {
//        return ageField;
//    }
//
//    /**
//     * @param ageField the ageField to set
//     */
//    public void setAgeField(AgeField ageField) {
//        this.ageField = ageField;
//    }
//
//    /**
//     * @return the ageSelection
//     */
//    public List<ExtendedFieldSet> getAgeSelection() {
//        return ageSelection;
//    }
//
//    /**
//     * @param ageSelection the ageSelection to set
//     */
//    public void setAgeSelection(List<ExtendedFieldSet> ageSelection) {
//        this.ageSelection = ageSelection;
//    }
//
//    /**
//     * @return the metric
//     */
//    public String getMeasure() {
//        return measure;
//    }
//
//    /**
//     * @param metric the metric to set
//     */
//    public void setMeasure(String measure) {
//        this.measure = measure;
//    }
//
//    /**
//     * @return the inputCohort
//     */
//    public String getInputCohort() {
//        return inputCohort;
//    }
//
//    /**
//     * @param inputCohort the inputCohort to set
//     */
//    public void setInputCohort(String inputCohort) {
//        this.inputCohort = inputCohort;
//    }
//
//    /**
//     * @return the outputCohort
//     */
//    public String getOutputCohort() {
//        return outputCohort;
//    }
//
//    /**
//     * @param outputCohort the outputCohort to set
//     */
//    public void setOutputCohort(String outputCohort) {
//        this.outputCohort = outputCohort;
//    }
//
//    /**
//     * @return the userId
//     */
//    public String getUserId() {
//        return userId;
//    }
//
//    /**
//     * @param userId the userId to set
//     */
//    public void setUserId(String userId) {
//        this.userId = userId;
//    }

    @JsonIgnore
    public boolean isValid() {
        return (birthSequence != null) &&
                (birthSequence.isValid()) &&
                (dataSource != null) &&
                (ageField != null) &&
                (measure != null);
    }

    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            LOG.info(e);
        }
        return null;
    }

    public String toPrettyString() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            LOG.info(e);
        }
        return null;
    }

}
