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
import com.nus.cool.core.schema.FieldType;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class UserCountAggregatorEvent implements EventAggregator{

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
        int age = 1;
        while (offset >= 0) {
            // determine the age
        	while (offset >= ageDelimiter.get(age)) age++;

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
			}

            // increment age and adjust offset
			age++;
			offset = ageOffset.nextSetBit(boffset);
			for (int i = 0; i < ageInterval && boffset >= 0; i++)
				boffset = ageDelimiter.nextSetBit(boffset + 1);
		}
	}

	@Override
	public void ageAggregate(BitSet ageOffset, InputVector timeVec, int birthDay, int ageOff, int ageEnd, int ageInterval,
							 TimeUnit unit, FieldFilter ageFilter, Map<Integer, List<Double>> ageMetrics) {
		// init the first day
		List<Double> cellValue = new ArrayList<>(1);
		cellValue.add(0.0);
		ageMetrics.put(0, cellValue);

		int ageDate = TimeUtils.getDateofNextTimeUnitN(birthDay, unit, 1);
		int toffset = TimeUtils.skipToDate(timeVec, ageOff, ageEnd, ageDate);
		int age = 0, offset = 0;
		while (toffset < ageEnd && offset >= 0) {
			// determine the age
			do {
				age++;
				offset = ageOffset.nextSetBit(toffset);
				if (offset < 0) return;
				ageDate = TimeUtils.getDateofNextTimeUnitN(ageDate, unit, ageInterval);
				toffset = TimeUtils.skipToDate(timeVec, toffset, ageEnd, ageDate);
			} while (!ageFilter.accept(age) || offset >= toffset);

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
	public void ageAggregateMetirc(BitSet ageOffset, InputVector timeVec, int birthDay, int ageOff, int ageEnd, int ageInterval,
								   TimeUnit unit, FieldFilter ageFilter, InputVector filedValue, Map<Integer, List<Double>> ageMetrics){
		// initialize the first age, i.e., the birth day
		int age = 0;
		List<Double> cohortCell = ageMetrics.get(age);
		if (cohortCell == null) {
			cohortCell = initCohortCell();
			ageMetrics.put(age, cohortCell);
		}
		cohortCell.set(0, cohortCell.get(0) + 1);

		// start from the birthday
		int ageDate = TimeUtils.getDateofNextTimeUnitN(birthDay, unit, age);
		int toffset = TimeUtils.skipToDate(timeVec, ageOff, ageEnd, ageDate);
		int offset, nextAgeDate, nextToffset;
		while (toffset < ageEnd) {
			// determine the age
			nextAgeDate = TimeUtils.getDateofNextTimeUnitN(ageDate, unit, ageInterval);
			nextToffset = TimeUtils.skipToDate(timeVec, toffset, ageEnd, nextAgeDate);
			offset = ageOffset.nextSetBit(toffset);
			if (offset < 0) return;

			boolean hasActivity = false;
			age = TimeUtils.getDateFromOffset(timeVec, offset)-birthDay;
			if (!ageFilter.accept(age)) return;
			cohortCell = ageMetrics.get(age);
			// init the cohort cell
			// if the cohort cell is for the metric, it consists of "measure", "max", "min", "sum", "num"
			// else, it only consists of "measure"
			if (cohortCell == null) {
				cohortCell = initCohortCell();
				ageMetrics.put(age, cohortCell);
			}
			// update the cohort cell
			while(offset>0 && offset<nextToffset){
				hasActivity = true;
				filedValue.skipTo(offset);
				int val = filedValue.next();
				updateStats(val, cohortCell);
				offset = ageOffset.nextSetBit(offset+1);
			}
			// add the measure by 1
			if (hasActivity)  ageMetrics.get(age).set(0,  ageMetrics.get(age).get(0) + 1);

			ageDate = nextAgeDate;
			toffset = nextToffset;
		}
	}

	private List<Double> initCohortCell(){
		List<Double> cohortCell = new ArrayList<>(5);
		cohortCell.add(0.0);
		cohortCell.add(Double.MAX_VALUE);
		cohortCell.add(-1.0 * Double.MAX_VALUE);
		cohortCell.add(0.0);
		cohortCell.add(0.0);
		return cohortCell;
	}

	private void updateStats(int val, List<Double> cohortCell) {
		if (val <cohortCell.get(1))
			cohortCell.set(1, (double)val);
		if (val > cohortCell.get(2))
			cohortCell.set(2, (double)val);
		cohortCell.set(3, cohortCell.get(3) + val);
		cohortCell.set(4, cohortCell.get(4) + 1);
	}

}
