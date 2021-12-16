/*
 * Copyright 2020 Cool Squad Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nus.cool.core.util;

/**
 * Missing functions in java Array
 *
 * @author zhongle
 * @version 0.1
 * @since 0.1
 */
public class ArrayUtil {

  /**
   * Find the max value in int array
   *
   * @param vec target int array
   * @return max value
   */
  public static int max(int[] vec) {
    int max = Integer.MIN_VALUE;
    for (int v : vec) {
      max = Math.max(max, v);
    }
    return max;
  }

  /**
   * Find the min value in int array
   *
   * @param vec target int array
   * @return min value
   */
  public static int min(int[] vec) {
    int min = Integer.MAX_VALUE;
    for (int v : vec) {
      min = Math.min(min, v);
    }
    return min;
  }
}
