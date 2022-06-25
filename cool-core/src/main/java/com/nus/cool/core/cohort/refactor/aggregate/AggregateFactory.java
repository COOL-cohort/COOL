package com.nus.cool.core.cohort.refactor.aggregate;

public class AggregateFactory {

    public static AggregateFunc generateAggregate(String str) {
        switch (str) {
            case "Average":
                new AverageFunc();
            case "Count":
                new CountFunc();
            case "Max":
                new MaxFunc();
            case "Min":
                new MinFunc();
            case "Sum":
                new SumFunc();
            default:
                throw new IllegalArgumentException(
                        String.format("%s this type is not existed ", str));
        }
    }
}
