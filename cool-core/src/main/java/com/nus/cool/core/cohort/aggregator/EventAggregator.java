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

public interface EventAggregator {

  void init(InputVector vec);

  Double birthAggregate(List<Integer> offset);

  /**
   * Aggregate over @param ageOffset with age defined by @param ageDelimiter.
   *
   * @param ageDelimiter a bitset with each birthday (including the birth time) 
   *      position set; there should be at least one set bit.
   */
  void ageAggregate(BitSet ageOffset, BitSet ageDelimiter, int ageOff, int ageEnd, int ageInterval,
      FieldFilter ageFilter, Map<Integer, List<Double>> ageMetrics);

  /**
   * Aggregate over @param ageOffset with age defined by time.
   * @param time time field
   */
  void ageAggregate(BitSet ageOffset, InputVector time, int birthDay, int ageOff, int ageEnd,
      int ageInterval, TimeUnit unit, FieldFilter ageFilter, Map<Integer, List<Double>> ageMetrics);

  /**
   * Aggregate over @param ageOffset with age defined by time for the metric field.
   *
   * @param time time field
   * @param fieldValue the field value of the metric field that is in the ageSelection filter
   */
  void ageAggregateMetirc(BitSet ageOffset, InputVector time, int birthDay, int ageOff, int ageEnd,
      int ageInterval, TimeUnit unit, FieldFilter ageFilter, InputVector fieldValue,
      Map<Integer, List<Double>> ageMetrics);
}
