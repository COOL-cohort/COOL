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

package com.nus.cool.core.cohort;

import java.util.ArrayList;
import java.util.List;

/**
 * Extended cohort representation.
 */
public class ExtendedCohort {

  private List<Integer> dimensions = new ArrayList<>();

  private List<String> dimentionNames = new ArrayList<>();

  private int birthOffset;

  private int birthDate;

  public ExtendedCohort() {
    birthOffset = -1;
  }

  public ExtendedCohort(ExtendedCohort that) {
    this.dimensions.addAll(that.dimensions);
    this.birthOffset = that.birthOffset;
  }

  public void addDimension(int dim) {
    this.dimensions.add(dim);
  }

  public void clearDimension() {
    this.dimensions.clear();
  }

  public void addDimensionName(String name) {
    this.dimentionNames.add(name);
  }

  public List<Integer> getDimensions() {
    return dimensions;
  }

  public int getBirthOffset() {
    return birthOffset;
  }

  public void setBirthOffset(int birthOffset) {
    this.birthOffset = birthOffset;
  }

  public int getBirthDate() {
    return birthDate;
  }

  public void setBirthDate(int birthDate) {
    this.birthDate = birthDate;
  }

  @Override
  public int hashCode() {
    return this.dimensions.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return this.dimensions.equals(((ExtendedCohort) obj).dimensions);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append('(');

    for (String dimName : this.dimentionNames) {
      builder.append(dimName);
      builder.append(", ");
    }
    if (!this.dimensions.isEmpty()) {
      if (builder.length() >= 2) {
        builder.delete(builder.length() - 2, builder.length());
      }
    } else {
      builder.append("cohort");
    }
    builder.append(')');
    return builder.toString();
  }
}
