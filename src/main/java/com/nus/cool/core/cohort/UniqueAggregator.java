package com.nus.cool.core.cohort;

import sg.edu.nus.comp.aeolus.core.io.InputVector;

import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class UniqueAggregator implements EventAggregator {

    private InputVector values;

    @Override
    public void init(InputVector vec) {
        this.values = vec;
    }

    @Override
    public Double birthAggregate(List<Integer> offset) {
        if (offset.isEmpty()) return null;
        boolean same = true;
        int ret = values.get(offset.get(0));
        for (Integer e : offset) {
            same &= (ret == values.get(e));
        }
        return same ? (double) ret : null;
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
