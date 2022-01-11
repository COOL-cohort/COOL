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
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * DateBase is the base date for all date-related storage
 */
public class DateBase {

  /**
   * Date formatter
   */

  public static final DateTimeFormatter FORMATTER;

  /** 
   * Reference day
   */
  public static final DateTime BASE;

  static {
    FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");
    BASE = FORMATTER.parseDateTime("1970-01-01");
  }
}
