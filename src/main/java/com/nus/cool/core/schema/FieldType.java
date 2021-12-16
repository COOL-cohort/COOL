package com.nus.cool.core.schema;

public enum FieldType {

    /**
     * This field type used to distinguish platform
     * for cohort query.
     */
    AppKey,

    /**
     * This field type used to distinguish user
     * for cohort query.
     */
    UserKey,

    /**
     * Date format data, store as numeric.
     */
    ActionTime,

    /**
     * String value
     */
    Action,

    /**
     * String value
     */
    Segment,

    /**
     * Numeric
     */
    Metric;

    public static FieldType fromInteger(int i) {
        switch (i) {
            case 0:
                return AppKey;
            case 1:
                return UserKey;
            case 2:
                return ActionTime;
            case 3:
                return Action;
            case 4:
                return Segment;
            case 5:
                return Metric;
            default:
                throw new IllegalArgumentException("Invalid field type int: " + i);
        }
    }
}