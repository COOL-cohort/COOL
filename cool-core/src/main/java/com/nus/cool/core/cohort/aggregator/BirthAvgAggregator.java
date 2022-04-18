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
import com.nus.cool.core.cohort.filter.FieldFilter;
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

    /**
     * @brief  get the average value of a list which is the cohort result at a specific age
     *
     * @param offset the list to get the average value
     */
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

    @Override
    public void ageAggregateMetirc(BitSet ageOffset, InputVector time, int birthDay, int ageOff, int ageEnd, int ageInterval,
                                   TimeUnit unit, FieldFilter ageFilter, InputVector filedValue, Map<Integer, List<Double>> ageMetrics){
        // TODO Auto-generated method stub
    }
}
