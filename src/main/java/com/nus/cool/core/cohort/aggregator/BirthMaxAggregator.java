package com.nus.cool.core.cohort.aggregator;


import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.cohort.TimeUnit;
import com.nus.cool.core.cohort.filter.FieldFilter;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class BirthMaxAggregator implements EventAggregator {

    private InputVector values;

    @Override
    public void init(InputVector vec) {
        values = vec;
    }

    @Override
    public Double birthAggregate(List<Integer> offset) {
    	if (offset.isEmpty()) return null;
        checkArgument(offset.size() > 0);
        long max = offset.get(0);
        for (Integer i : offset) {
            long v = this.values.get(i);
            max = (max < v) ? v : max;
        }
        return (double) max;
    }


	@Override
	public void ageAggregate(BitSet ageOffset, BitSet ageDelimiter, int start, int end, int ageInterval,
			FieldFilter ageFilter, Map<Integer, List<Double>> ageMetrics) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void ageAggregate(BitSet ageOffset, InputVector time, int birthDay, int ageOff, int ageEnd, int ageInterval,
			TimeUnit unit, FieldFilter ageFilter, Map<Integer, List<Double>> ageMetrics) {
		// TODO Auto-generated method stub
		
	}
}
