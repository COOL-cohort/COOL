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

package com.nus.cool.core.util;

/**
 * ArrayUtil contains the missing functions we need to use in java Array.
 * The class contains the function to get the max and min value of an Array
 */
public class ArrayUtil {

  /**
   * Find the max value in int array.
   *
   * @param vec target int array
   * @return the max value
   */
  public static int max(int[] vec) {
    int max = Integer.MIN_VALUE;
    for (int v : vec) {
      max = Math.max(max, v);
    }
    return max;
  }

  /**
   * Find the min value in int array.
   *
   * @param vec target int array
   * @return the min value
   */
  public static int min(int[] vec) {
    int min = Integer.MAX_VALUE;
    for (int v : vec) {
      min = Math.min(min, v);
    }
    return min;
  }
}
