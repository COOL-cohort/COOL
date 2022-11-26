package com.nus.cool.core.cohort.aggregate;

/**
 * Generate different aggregators according to cohort query.
 * 
 * <p>
 * AverageAggregate: calculate average value of selected schema for all
 * traversed eligitable tuple COUNT: calculate the number for all traversed
 * eligiable tuple MAX: keep the max value of selected schema in all traversed
 * eligiable tuple MIN: keep the min value of selected schema in all traversed
 * eligiable tuple SUM: sum up all value of selected schema for all traversed
 * eligiable tuple DISTINCT: count the distinct value of selected schema for all
 * traversed eligiable tuple
 */
public class AggregateFactory {

  /**
   * deserialize AggregateFunc.
   */
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
