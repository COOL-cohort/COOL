/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.nus.cool.core.cohort.aggregator;

import com.nus.cool.core.cohort.TimeUnit;
import com.nus.cool.core.cohort.TimeUtils;
import com.nus.cool.core.cohort.filter.FieldFilter;
import com.nus.cool.core.io.storevector.InputVector;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class RollingRentionAggregator implements EventAggregator{

    @Override
    public void init(InputVector vec) {
    }

    @Override
    public Double birthAggregate(List<Integer> offset) {
        throw new UnsupportedOperationException();
    }

    public void ageAggregate(BitSet ageOffset, List<Integer> ageDelimiter, 
            Map<Integer, List<Double>> ageMetrics) {

        int offset = ageOffset.nextSetBit(ageDelimiter.get(0));
        int age = 1, prevAge = 0;
        while (offset >= 0) {
            // determine the age
        	while (offset >= ageDelimiter.get(age)) age++;

        	// no need to proceed once the this user is not retained in the current age
        	if (age > prevAge + 1) break;

            List<Double> metric = ageMetrics.get(age);
            if (metric == null) {
                metric = new ArrayList<Double>(1);
                metric.add(new Double(0));
                ageMetrics.put(age, metric);
            }

            int v = metric.get(0).intValue();
            ++v;
            while (offset >= 0 && offset < ageDelimiter.get(age)) {
                offset = ageOffset.nextSetBit(offset + 1);
            }

            metric.set(0, (double) v);
        }
    }
    
	@Override
	public void ageAggregate(BitSet ageOffset, BitSet ageDelimiter, int ageOff, int ageEnd, int ageInterval,
							 FieldFilter ageFilter, Map<Integer, List<Double>> ageMetrics) {
		
		int offset = ageOffset.nextSetBit(ageOff);
		int age = 1;
        int boffset = ageDelimiter.nextSetBit(ageOff);
		for (int i = 0; i < ageInterval - 1 && boffset >= 0; i++)
			boffset = ageDelimiter.nextSetBit(boffset + 1);

		while (offset >= 0 && boffset >= 0) {
			// compute aggregation for the current age
			if (ageFilter.accept(age) && offset < boffset) {
				List<Double> metric = ageMetrics.get(age);
				if (metric == null) {
					metric = new ArrayList<Double>(1);
					metric.add(new Double(0));
					ageMetrics.put(age, metric);
				}
				metric.set(0, metric.get(0) + 1);
			} else {
				// this user is not retained in the current age, no need to proceed
				break;
			}

            // increment age and adjust offset
			age++;
			offset = ageOffset.nextSetBit(boffset);
			for (int i = 0; i < ageInterval && boffset >= 0; i++)
				boffset = ageDelimiter.nextSetBit(boffset + 1);
		}
	}

	@Override
	public void ageAggregate(BitSet ageOffset, InputVector time, int birthDay, int ageOff, int ageEnd, int ageInterval,
							 TimeUnit unit, FieldFilter ageFilter, Map<Integer, List<Double>> ageMetrics) {
		//skip to the first day
        int ageDate = TimeUtils.getDateofNextTimeUnitN(birthDay, unit, 1);
        int toffset = TimeUtils.skipToDate(time, ageOff, ageEnd, ageDate);
        int age = 0, offset = 0;
        while (toffset < ageEnd && offset >= 0) {
        	
        	// go to the next age
        	age++;
        	offset  = ageOffset.nextSetBit(toffset);
			ageDate = TimeUtils.getDateofNextTimeUnitN(ageDate, unit, ageInterval);
			toffset = TimeUtils.skipToDate(time, toffset, ageEnd, ageDate);
			
			// check if the current user is retained or not at this age
			if (offset < 0 || !ageFilter.accept(age) || offset >= toffset)
				// not retained; return immediately
				break;
			
			// update the metric accordingly
			List<Double> metric = ageMetrics.get(age);
			if (metric == null) {
				metric = new ArrayList<Double>(1);
				metric.add(new Double(0));
				ageMetrics.put(age, metric);
			}
			metric.set(0, metric.get(0) + 1);		
        }		
	}

	@Override
	public void ageAggregateMetirc(BitSet ageOffset, InputVector time, int birthDay, int ageOff, int ageEnd, int ageInterval,
								   TimeUnit unit, FieldFilter ageFilter, InputVector filedValue, Map<Integer, List<Double>> ageMetrics){
		// TODO Auto-generated method stub
	}

	@Override
	public void ageAggregateMetirc(BitSet bv, InputVector valueVector, int birthDate, int ageOff, int end, int ageInterval, TimeUnit unit, FieldFilter ageFieldFilter, int totalUsers, InputVector fieldIn, Map<Integer, List<Double>> cohortCells) {

	}
}
