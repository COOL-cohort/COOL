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

package com.nus.cool.core.cohort;

import com.nus.cool.core.cohort.aggregator.BirthAvgAggregator;
import com.nus.cool.core.cohort.aggregator.BirthCountAggregator;
import com.nus.cool.core.cohort.aggregator.BirthMaxAggregator;
import com.nus.cool.core.cohort.aggregator.BirthMinAggregator;
import com.nus.cool.core.cohort.aggregator.BirthSumAggregator;
import com.nus.cool.core.cohort.aggregator.EventAggregator;
import com.nus.cool.core.cohort.aggregator.RollingRentionAggregator;
import com.nus.cool.core.cohort.aggregator.UniqueAggregator;
import com.nus.cool.core.cohort.aggregator.UserCountAggregatorEvent;
import java.util.HashMap;
import java.util.Map;

/**
 * Factory class of birth aggregators.
 */
public class BirthAggregatorFactory {

  private static final Map<String, EventAggregator> aggregators = new HashMap<>();

  static {
    aggregators.put("UNIQUE", new UniqueAggregator());
    aggregators.put("COUNT", new BirthCountAggregator());
    aggregators.put("RETENTION", new UserCountAggregatorEvent());
    aggregators.put("SUM", new BirthSumAggregator());
    aggregators.put("AVG", new BirthAvgAggregator());
    aggregators.put("MAX", new BirthMaxAggregator());
    aggregators.put("MIN", new BirthMinAggregator());
    aggregators.put("ROLLRETENTION", new RollingRentionAggregator());
  }

  public static EventAggregator getAggregator(String op) {
    return aggregators.get(op);
  }
}
