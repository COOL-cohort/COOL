package com.nus.cool.core.cohort.aggregator;

import com.nus.cool.core.io.storevector.InputVector;

import java.util.BitSet;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class SumAggregator implements Aggregator {

    private InputVector eventField;

    private InputVector metricField;

    private int ageDivider;

    private int from;

    private int to;

    @Override
    public void init(InputVector metricVec, InputVector eventDayVec, int maxAges, int from, int to, int ageDivider) {
        checkArgument(from >= 0 && from <= to);
        this.metricField = checkNotNull(metricVec);
        this.eventField = checkNotNull(eventDayVec);
        this.ageDivider = ageDivider;
        this.from = from;
        this.to = to;
    }

    @Override
    public void processUser(BitSet hitBV, int sinceDay, int start, int end, long[] row) {
        for (int i = start; i < end; i++) {
            int nextPos = hitBV.nextSetBit(i);
            if (nextPos < 0)
                return;
            this.eventField.skipTo(nextPos);
            int curDay = this.eventField.next();
            int age = (curDay - sinceDay) / this.ageDivider;
            if (age <= 0 || age < this.from)
                continue;
            if (age > this.to || age >= row.length)
                break;
            this.metricField.skipTo(nextPos);
            row[age] += this.metricField.next();
            i = nextPos;
        }
    }
}
