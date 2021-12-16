package com.nus.cool.core.cohort.aggregator;

import com.nus.cool.core.io.storevector.InputVector;

import java.util.BitSet;

public interface Aggregator {

    void init(InputVector metricVec, InputVector eventDayVec, int maxAges, int from, int to, int ageDivider);

    void processUser(BitSet hitBV, int sinceDay, int start, int end, long[] row);
}
