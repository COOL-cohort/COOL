/**
 * 
 */
package com.nus.cool.core.cohort;

import java.util.ArrayList;
import java.util.List;

/**
 * Field is mainly used to represent the conditions we set up for extended queries
 *
 */
public class ExtendedFieldSet {
    
    public static enum FieldValueType {
        
        AbsoluteValue,

        IncreaseByAbsoluteValue,

        IncreaseByPercentage,

        Equal,

        Inequal

    }

    public static enum FieldSetType {
        
        Set,
        
        Range

    }

    public static class FieldValue {

        private FieldValueType type = FieldValueType.AbsoluteValue;

        private List<String> values = new ArrayList<>(0);

        private String baseField;

        private int baseEvent = -1;

        /**
         * @return the type
         */
        public FieldValueType getType() {
            return type;
        }

        /**
         * @param type the type to set
         */
        public void setType(FieldValueType type) {
            this.type = type;
        }

        /**
         * @return the value
         */
        public List<String> getValues() {
            return values;
        }

        /**
         * @param values the value to set
         */
        public void setValues(List<String> values) {
        	if (values != null)
        		this.values = values;
        }

        /**
         * @return the baseField
         */
        public String getBaseField() {
            return baseField;
        }

        /**
         * @param baseField the baseField to set
         */
        public void setBaseField(String baseField) {
            this.baseField = baseField;
        }

        /**
         * @return the baseEvent
         */
        public int getBaseEvent() {
            return baseEvent;
        }

        /**
         * @param baseEvent the baseEvent to set
         */
        public void setBaseEvent(int baseEvent) {
            this.baseEvent = baseEvent;
        }
    }
    
    private FieldSetType setType;
    
    private String field;
    
    private FieldValue fieldValue;
    
    /**
     * @return the setType
     */
    public FieldSetType getFilterType() {
        return setType;
    }

    /**
     * @param setType the setType to set
     */
    public void setFilterType(FieldSetType setType) {
        this.setType = setType;
    }

    /**
     * @return the field
     */
    public String getCubeField() {
        return field;
    }

    /**
     * @param field the field to set
     */
    public void setCubeField(String field) {
        this.field = field;
    }

    /**
     * @return the values
     */
    public FieldValue getFieldValue() {
        return fieldValue;
    }

    /**
     * @param value the values to set
     */
    public void setFieldValue(FieldValue value) {
        this.fieldValue = value;
    }
    
}
