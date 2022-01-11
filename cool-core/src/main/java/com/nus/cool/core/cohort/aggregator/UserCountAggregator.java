/*
 * Copyright 2021 Cool Squad Team
 *
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.nus.cool.core.io.storevector.InputVector;
import java.util.BitSet;

/**
 * UserCountAggregator is used to aggregate the results of users at different time
 * when the metric is retention
 */
public class UserCountAggregator implements Aggregator {

  /**
   * The field of action time
   */
  private InputVector eventDayVec;

  /**
   * Indicates whether the user contain the corresponding result at each age
   */
  private BitSet mask;

  /**
   * Regard how many days or weeks or others as an age 
   */
  private int ageDivider;

  /**
   * The start position of the user in the table
   */
  private int from;


  /**
   * The end position of the user in the table
   */
  private int to;

  /**
   * Initiate UserCountAggregator with the configuration of cohort analysis
   *
   * @param metricVec the metric field
   * @param eventDayCev the action time field
   * @param maxAges the number of ages we set up
   * @param from the start position of the user in the table
   * @param to the end position of the user in the table
   * @param ageDivider how many days or weeks or others as an age 
   */
  @Override
  public void init(InputVector metricVec, InputVector eventDayVec, int maxAges, int from, int to,
      int ageDivider) {
    checkArgument(from >= 0 && from <= to);
    this.eventDayVec = checkNotNull(eventDayVec);
    this.mask = new BitSet(maxAges);
    this.ageDivider = checkNotNull(ageDivider);
    this.from = from;
    this.to = to;
  }

  /**
   * Get the Bitset to indicate whether the user contain the corresponding result at each age
   *
   * @param hitBV the bitset that indicates which record in the table is effective
   * @param sinceDay the birth time of the user
   * @param start the position for the first age tuple of the user
   * @param end the end position of the user's age tuples in the table
   * @param row the array that store the aggregation result
   */
  @Override
  public void processUser(BitSet hitBV, int sinceDay, int start, int end, long[] row) {
    this.mask.clear();
    this.eventDayVec.skipTo(start);
    for (int i = start; i < end; i++) {
      int eventDay = this.eventDayVec.next();
      if (hitBV.get(i)) {
        int age = (eventDay - sinceDay) / this.ageDivider;
        if (age <= 0 || age >= row.length || this.mask.get(age)) {
          continue;
        }
        if (age < this.from) {
          continue;
        }
        if (age > to) {
          break;
        }
        row[age]++;
        this.mask.set(age);
        break;
      }
    }
  }
}
