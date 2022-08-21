package com.nus.cool.core.cohort.refactor.aggregate;
/**
 * Generate different aggregators according to cohort query
 * 
 * AverageAggregate:
 *      calculate average value of selected schema for all traversed eligitable tuple
 * COUNT:
 *      calculate the number for all traversed eligiable tuple
 * MAX:
 *      keep the max value of selected schema in all traversed eligiable tuple
 * MIN:
 *      keep the min value of selected schema in all traversed eligiable tuple
 * SUM:
 *      sum up all value of selected schema for all traversed eligiable tuple
 * DISTINCT:
 *      count the distinct value of selected schema for all traversed eligiable tuple
 */
public class AggregateFactory {

    public static AggregateFunc generateAggregate(AggregateType type, String schema) {
        switch (type) {
            case AVERAGE:
                return AverageAggregate.getInstance();
            case COUNT:
                return CountAggregate.getInstance();
            case MAX:
                return MaxAggregate.getInstance();
            case MIN:
                return MinAggregate.getInstance();
            case SUM:
                return SumAggregate.getInstance();
            case DISTINCT:
                return DistinctCountAggregate.getInstance();
            default:
                throw new IllegalArgumentException(
                        String.format("%s this type is not existed ", type.toString()));
        }
    }
}
