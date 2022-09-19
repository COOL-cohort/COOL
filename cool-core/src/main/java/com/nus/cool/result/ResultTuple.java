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

package com.nus.cool.result;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * ResultTuple is used to store the result, including cohort name and age.
 */
@Data
@AllArgsConstructor
public class ResultTuple {

  /**
   * cohort name.
   */
  private String cohort;

  /**
   * age of the cohort.
   */
  private int age;

  /**
   * measure of interest.
   */
  private long measure;

  /**
   * Merge query results to a distinct set of cohorts.
   *
   * @param resultTuples a list of partial query results
   * @return merged results
   */
  public static List<ResultTuple> merge(List<ResultTuple> resultTuples) {
    Map<String, Map<Integer, Long>> resMap = new HashMap<>();
    for (ResultTuple res : resultTuples) {
      if (resMap.get(res.cohort) == null) {
        Map<Integer, Long> map = new HashMap<>();
        map.put(res.age, res.measure);
        resMap.put(res.cohort, map);
      } else if (resMap.get(res.cohort).get(res.age) == null) {
        Map<Integer, Long> map = resMap.get(res.cohort);
        map.put(res.age, res.measure);
      } else {
        // aggregates the measure of the same cohort with the same age.
        Map<Integer, Long> map = resMap.get(res.cohort);
        map.put(res.age, res.measure + map.get(res.age));
      }
    }
    List<ResultTuple> results = new ArrayList<>();
    for (Map.Entry<String, Map<Integer, Long>> entry : resMap.entrySet()) {
      String cohort = entry.getKey();
      Map<Integer, Long> map = entry.getValue();
      for (Map.Entry<Integer, Long> entry1 : map.entrySet()) {
        int age = entry1.getKey();
        long measure = entry1.getValue();
        ResultTuple res = new ResultTuple(cohort, age, measure);
        results.add(res);
      }
    }
    return results;
  }
}
