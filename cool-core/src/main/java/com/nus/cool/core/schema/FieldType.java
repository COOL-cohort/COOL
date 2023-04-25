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

package com.nus.cool.core.schema;

/**
 * FieldType defines the types of fields.
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
   * String value.
   */
  Action,

  /**
   * String value.
   */
  Segment,

  /**
   * Numeric.
   */
  Metric,

  /**
   * Float.
   */
  Float;

  /**
   * Translate an integer to its corresponding field type.
   */
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
      case 6:
        return Float;
      default:
        throw new IllegalArgumentException("Invalid field type int: " + i);
    }
  }

  /**
   * Check if a type is a hash type.
   */
  public static boolean isHashType(FieldType fieldType) {
    switch (fieldType) {
      case Action:
      case AppKey:
      case Segment:
      case UserKey:
        return true;
      case ActionTime:
      case Metric:
      case Float:
        return false;
      default:
        throw new IllegalArgumentException("Invalid field" + fieldType.toString());
    }
  }
}
