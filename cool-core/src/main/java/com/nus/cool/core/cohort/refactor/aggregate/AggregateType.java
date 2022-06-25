package com.nus.cool.core.cohort.refactor.aggregate;

public enum AggregateType {
    AVERAGE("Average"), COUNT("Count"),
    MAX("Max"),MIN("Min"),SUM("Sum");

    private final String text;

    AggregateType(final String text) {
        this.text =text;
    }

    @Override
    public String toString(){
        return text;
    }

    public static AggregateType parsefrom(String str){
        switch(str){
            case "Average": return AVERAGE;
            case "Count": return COUNT;
            case "Max":return MAX;
            case "Min":return MIN;
            case "Sum":return SUM;
            default:
                throw new IllegalArgumentException(
                        String.format("%s this type is not existed ", str));
        }
    }
}
