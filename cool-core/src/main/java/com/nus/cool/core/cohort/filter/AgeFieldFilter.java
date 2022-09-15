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

package com.nus.cool.core.cohort.filter;

import static com.google.common.base.Preconditions.checkArgument;

import com.nus.cool.core.cohort.ExtendedFieldSet;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.FieldType;
import java.util.List;

/**
 * Age field filter.
 */
public class AgeFieldFilter implements FieldFilter {

  private final int minAge;

  private final int maxAge;

  /**
   * Construct an filter on age field.

   * @param values the [min, max] accepted value range
   */
  public AgeFieldFilter(List<String> values) {
    checkArgument(values != null && !values.isEmpty());
    String[] range = values.get(0).split("\\|");
    this.minAge = Integer.parseInt(range[0]);
    this.maxAge = Integer.parseInt(range[1]);
  }

  @Override
  public int getMinKey() {
    return minAge;
  }

  @Override
  public int getMaxKey() {
    return maxAge;
  }

  @Override
  public boolean accept(MetaFieldRS metaField) {
    return true;
  }

  @Override
  public boolean accept(FieldRS chunkField) {
    return true;
  }

  @Override
  public boolean accept(InputVector inputVector) {
    return true;
  }

  @Override
  public boolean accept(int v) {
    return (v >= minAge && v <= maxAge);
  }

  @Override
  public List<String> getValues() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ExtendedFieldSet getFieldSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateValues(Double v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int nextAcceptTuple(int start, int to) {
    throw new UnsupportedOperationException();
  }

  @Override
  public FieldType getFieldType() {
    return fieldType;
  }

}
