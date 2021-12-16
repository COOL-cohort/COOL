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
package com.nus.cool.core.schema;

/**
 *
 * @author zhongle
 * @version 0.1
 * @since 0.1
 */
public enum FieldType {

  /**
   * This field type used to distinguish platform for cohort query.
   */
  AppKey,

  /**
   * This field type used to distinguish user for cohort query.
   */
  UserKey,

  /**
   * Date format data, store as numeric.
   */
  ActionTime,

  /**
   * String value
   */
  Action,

  /**
   * String value
   */
  Segment,

  /**
   * Numeric
   */
  Metric;

  public static FieldType fromInteger(int i) {
    switch (i) {
      case 0:
        return AppKey;
      case 1:
        return UserKey;
      case 2:
        return ActionTime;
      case 3:
        return Action;
      case 4:
        return Segment;
      case 5:
        return Metric;
      default:
        throw new IllegalArgumentException("Invalid field type int: " + i);
    }
  }
}