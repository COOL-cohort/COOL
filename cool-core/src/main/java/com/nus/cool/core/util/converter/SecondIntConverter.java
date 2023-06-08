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

package com.nus.cool.core.util.converter;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Seconds;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * SecondIntConverter converts the input day represented in format yyyy-MM-dd to integer
 * which is the number of days past the reference day.
 */
public class SecondIntConverter implements ActionTimeIntConverter {

  /**
   * Date formatter.
   */
  public static final DateTimeFormatter FORMATTER
      = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC();

  /**
   * Reference day.
   */
  public static final DateTime BASE
      = FORMATTER.parseDateTime("1970-01-01 00:00:00").withZone(DateTimeZone.UTC);

  public static final ActionTimeIntConverter getInstance() {
    // return x -> Days.daysBetween(BASE, FORMATTER.parseDateTime(x)).getDays();
    return x -> Seconds.secondsBetween(BASE, FORMATTER.parseDateTime(x)).getSeconds();
  }

  /**
   * Convert date string value to the number of days past the reference day.
   *
   * @param v date string value
   * @return number of days past the reference day
   */
  @Override
  public int toInt(String v) {
    DateTime end = FORMATTER.parseDateTime(v);
    return Seconds.secondsBetween(BASE, end).getSeconds();
  }

  /**
   * Get date according to number of days past the reference day.
   *
   * @param seconds number of seconds past the reference day
   * @return date string value for specific format
   */
  public String getString(int seconds) {
    DateTime dt = BASE.plusSeconds(seconds);
    return FORMATTER.print(dt);
  }
}
