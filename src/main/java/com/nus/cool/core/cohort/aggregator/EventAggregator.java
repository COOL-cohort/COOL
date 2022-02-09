package com.nus.cool.core.cohort.aggregator;

import com.nus.cool.core.cohort.TimeUnit;
import com.nus.cool.core.cohort.filter.FieldFilter;
import com.nus.cool.core.io.storevector.InputVector;

import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 *
 */
public interface EventAggregator {

    void init(InputVector vec);

    Double birthAggregate(List<Integer> offset);

    /**
     * @brief  aggregate over @param ageOffset with age defined by @param
     * ageDelimiter
     *
     * @param ageOffset
     * @param ageDelimiter a bitset with each birthday (including the birth time) 
     *      position set; there should be at least one set bit.
     * @param ageMetrics
     */
    void ageAggregate(BitSet ageOffset, BitSet ageDelimiter, int ageOff, int ageEnd,
                      int ageInterval, FieldFilter ageFilter, Map<Integer, List<Double>> ageMetrics);

    /**
     * @brief  aggregate over @param ageOffset with age defined by time
     *
     * @param ageOffset 
     * @param time time field
     * @param ageMetrics
     */
    void ageAggregate(BitSet ageOffset, InputVector time, int birthDay, int ageOff, int ageEnd,
                      int ageInterval, TimeUnit unit, FieldFilter ageFilter, Map<Integer, List<Double>> ageMetrics);

}
