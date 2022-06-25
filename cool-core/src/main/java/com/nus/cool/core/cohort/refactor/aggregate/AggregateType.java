package com.nus.cool.core.cohort.refactor.aggregate;

public enum AggregateType {
    AVERAGE("Average"), COUNT("Count"),
    MAX("Max"), MIN("Min"), SUM("Sum");

    private final String text;

    AggregateType(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }

}
