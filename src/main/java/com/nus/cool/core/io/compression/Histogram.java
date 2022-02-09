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
package com.nus.cool.core.io.compression;

import com.nus.cool.core.schema.CompressType;
import lombok.Builder;
import lombok.Getter;

/**
 * Properties for compress
 */
@Getter
@Builder
public class Histogram {

  /**
   * Compress data size
   */
  private int rawSize;

  /**
   * Means data value occurs in many consecutive data elements
   */
  private boolean sorted;

  /**
   * Number of values if compress data is countable
   */
  private int numOfValues;

  /**
   * Max(Last) value in compress data
   */
  private long max;

  /**
   * Min(First) value in compress data
   */
  private long min;

  /**
   * Specific compress type
   */
  private CompressType type;

  public Histogram count(int numOfValues) {
    this.numOfValues = numOfValues;
    return this;
  }
}
