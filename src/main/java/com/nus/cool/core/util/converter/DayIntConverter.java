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
package com.nus.cool.core.util.converter;

import org.joda.time.DateTime;
import org.joda.time.Days;

/**
 * DayIntConverter converts the input day represented in format yyyy-MM-dd
 * to integer which is the number of days past the reference day
 */
public class DayIntConverter implements NumericConverter {

  /**
   * Convert date string value to the number of days past the reference day.
   *
   * @param v date string value
   * @return number of days past the reference day
   */
  public int toInt(String v) {
    DateTime end = DateBase.FORMATTER.parseDateTime(v);
    return Days.daysBetween(DateBase.BASE, end).getDays();
  }

  /**
   * Get date according to number of days past the reference day.
   *
   * @param days number of days past the reference day
   * @return date string value for specific format
   */
  public String getString(int days) {
    DateTime dt = DateBase.BASE.plusDays(days);
    return DateBase.FORMATTER.print(dt);
  }
}
