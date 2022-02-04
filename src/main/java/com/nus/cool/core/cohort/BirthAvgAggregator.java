package com.nus.cool.core.cohort;


import com.nus.cool.core.io.storevector.InputVector;

import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class BirthAvgAggregator implements EventAggregator {

    private InputVector values;

    @Override
    public void init(InputVector vec) {
        values = vec;
    }

    @Override
    public Double birthAggregate(List<Integer> offset) {
    	if (offset.isEmpty()) return null;
        double r = 0;
        for (Integer i : offset) {
            r += values.get(i);
        }
        return r/offset.size();
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
