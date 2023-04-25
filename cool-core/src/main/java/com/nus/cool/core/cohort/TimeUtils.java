///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//
//package com.nus.cool.core.cohort;
//
//import static com.google.common.base.Preconditions.checkArgument;
//
//import com.nus.cool.core.cohort.utils.TimeUtils.TimeUnit;
//import com.nus.cool.core.io.storevector.InputVector;
//import org.joda.time.DateTime;
//import org.joda.time.Days;
//
///**
// * Utility class to manage time representation in cool.
// */
//public class TimeUtils {
//  /**
//   * Skip to the offset which corresponds to the first activity after the given date.
//   *
//   * @return the offset of the first activity; or endOffset if not found
//   */
//  public static int skipToDate(InputVector vector, int fromOffset, int endOffset, int date) {
//    // checkArgument(fromOffset <= endOffset);
//    vector.skipTo(fromOffset);
//    while (fromOffset < endOffset) {
//      if (getDate(vector.next()) >= date) {
//        break;
//      }
//      ++fromOffset;
//    }
//    return fromOffset;
//  }
//
//  public static int getDateFromOffset(InputVector vector, int offset) {
//    vector.skipTo(offset);
//    return getDate((Integer) vector.next());
//  }
//
//  public static int getDate(int days) {
//    return days;
//  }
//
//  /**
//   * Calculate the time N time units after a start time.
//   */
//  public static int getDateofNextTimeUnitN(int startDate, TimeUnit unit, int nextN) {
//    checkArgument(nextN >= 0);
//    switch (unit) {
//      case DAY:
//        return startDate + nextN;
//      case WEEK:
//        DateTime now = DateBase.BASE.plusDays(startDate);
//        return Days.daysBetween(DateBase.BASE, now.plusWeeks(nextN).withDayOfWeek(1)).getDays();
//      case MONTH:
//        now = DateBase.BASE.plusDays(startDate);
//        return Days.daysBetween(DateBase.BASE, now.plusMonths(nextN).withDayOfMonth(1)).getDays();
//      default:
//        throw new IllegalArgumentException("Unknown time unit: " + unit);
//    }
//  }
//
//  public static int skipToNextTimeUnitN(InputVector vector, TimeUnit unit, int fromOffset,
//                                        int endOffset, int startDate, int nextN) {
//    return skipToDate(vector, fromOffset, endOffset,
//        getDateofNextTimeUnitN(startDate, unit, nextN));
//  }
//}
