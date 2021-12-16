package com.nus.cool.core.cohort.aggregator;

import com.nus.cool.core.io.storevector.InputVector;

import java.util.BitSet;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class UserCountAggregator implements Aggregator {

    private InputVector eventDayVec;

    private BitSet mask;

    private int ageDivider;

    private int from;

    private int to;

    @Override
    public void init(InputVector metricVec, InputVector eventDayVec, int maxAges, int from, int to, int ageDivider) {
        checkArgument(from >= 0 && from <= to);
        this.eventDayVec = checkNotNull(eventDayVec);
        this.mask = new BitSet(maxAges);
        this.ageDivider = checkNotNull(ageDivider);
        this.from = from;
        this.to = to;
    }

    @Override
    public void processUser(BitSet hitBV, int sinceDay, int start, int end, long[] row) {
        this.mask.clear();
        this.eventDayVec.skipTo(start);
        for (int i = start; i < end; i++) {
            int eventDay = this.eventDayVec.next();
            if (hitBV.get(i)) {
                int age = (eventDay - sinceDay) / this.ageDivider;
                if (age <= 0 || age >= row.length || this.mask.get(age))
                    continue;
                if (age < this.from)
                    continue;
                if (age > to)
                    break;
                row[age]++;
                this.mask.set(age);
                break;
            }
        }
    }
}
