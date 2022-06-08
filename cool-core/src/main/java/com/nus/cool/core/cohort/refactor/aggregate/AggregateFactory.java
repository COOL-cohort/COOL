package com.nus.cool.core.cohort.refactor.aggregate;

public class AggregateFactory {

    public static AggregateFunc generateAggregate(AggregateType type, String schema) {
        switch (type) {
            case AVERAGE:
                return new AverageAggregate(schema);
            case COUNT:
                return new CountAggregate();
            case MAX:
                return new MaxAggregate(schema);
            case MIN:
                return new MinAggregate(schema);
            case SUM:
                return new SumAggregate(schema);
            case DISTINCT:
                return new DistinctCountAggregate(schema);
            default:
                throw new IllegalArgumentException(
                        String.format("%s this type is not existed ", type.toString()));

        }
    }
}
