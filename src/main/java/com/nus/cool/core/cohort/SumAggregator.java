/**
 * 
 */
package com.nus.cool.core.cohort;


import com.nus.cool.core.cohort.aggregator.Aggregator;
import com.nus.cool.core.io.storevector.InputVector;

import java.util.BitSet;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author david
 *
 */
public class SumAggregator implements Aggregator {
	
	private InputVector eventField;
	
	private InputVector metricField;
	
	private int ageDivider;
	
	private int from;

	private int to;

	@Override
	public void init(InputVector metricVec, InputVector eventDayVec, int maxAges,
			int from, int to,
			int ageDivider) {
		checkArgument(from >= 0 && from <= to);
		this.metricField = checkNotNull(metricVec);
		this.eventField = checkNotNull(eventDayVec);
		this.ageDivider = ageDivider;
		this.from = from;
		this.to = to;
	}

	@Override
	public void processUser(BitSet hitBV, int birthDay, int start, int end,
			long[] row) {
		
		//eventField.skipTo(start);
		//metricField.skipTo(start);
		for (int i = start; i < end; i++) {
			int nextPos = hitBV.nextSetBit(i);
			if(nextPos < 0)
				return;
				
			eventField.skipTo(nextPos);
			
			int curDay = eventField.next();
			
			int age = (curDay - birthDay) / ageDivider;
			
			if (age <= 0 || age < from)
				continue;
			
			if (age > to || age >= row.length)
				break;
			
//			if(age < from || age > to)
//				continue;
			
			metricField.skipTo(nextPos);
			row[age] += metricField.next();
			i = nextPos;
		}
	}

	@Override
	public void complete() {
	}

}
