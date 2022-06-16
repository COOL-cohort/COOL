package com.nus.cool.core.cohort.refactor.filter;

public enum FilterType {
    Range("Range"), Set("Set");

    private final String text;

    FilterType(final String text) {
        this.text =text;
    }

    @Override
    public String toString(){
        return text;
    }
}
