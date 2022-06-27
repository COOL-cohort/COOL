package com.nus.cool.core.cohort.refactor.aggregate;

public class AggregateFactory {

    public static AggregateFunc generateAggregate(AggregateType type) {
        switch (type) {
            case AVERAGE:
                return new AverageFunc();
            case COUNT:
                return new CountFunc();
            case MAX:
                return new MaxFunc();
            case MIN:
                return new MinFunc();
            case SUM:
                return new SumFunc();
            default:
                throw new IllegalArgumentException(
                        String.format("%s this type is not existed ", type.toString()));

        }
    }
}
