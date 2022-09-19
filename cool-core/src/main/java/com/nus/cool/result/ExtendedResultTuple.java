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

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * ResultTuple is used to store the result, including cohort name and age.
 */
@Data
@AllArgsConstructor
public class ExtendedResultTuple {

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
  private double measure;

  private double min;

  private double max;

  private double sum;

  private double num;

  public ExtendedResultTuple() {
  }

  @Override
  public String toString() {
    return String.format("cohort=%s, age=%s, measure=%s, min=%s, max=%s, sum=%s, num=%s,",
        cohort, age, measure, min, max, sum, num);
  }
}
