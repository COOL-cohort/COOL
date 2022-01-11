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
package com.nus.cool.core.cohort.filter;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Lists;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.util.ArrayUtil;
import com.nus.cool.core.util.converter.NumericConverter;
import com.nus.cool.core.util.parser.TupleParser;
import com.nus.cool.core.util.parser.VerticalTupleParser;
import java.util.List;

/**
 * RangeFieldFilter is used to get the range of the values of the field
 * and check whether the input is eligible
 * Usage: first check whether the metafield is eligible
 * Then find the eligible tuples of the field
 */
public class RangeFieldFilter implements FieldFilter {

  /**
   * The array stores the local minimum of the conditions
   */
  private int[] minValues;

  /**
   * The array stores the local maximum of the conditions
   */
  private int[] maxValues;

  /**
   * The global minimum of the conditions
   */
  private int min;

   /**
   * The global maximum of the conditions
   */
  private int max;

  /**
   * Get the range of the field
   *
   * @param values the values of the conditions
   * @param converter the converter to convert the values to interger
   */
  public RangeFieldFilter(List<String> values, NumericConverter converter) {
    checkNotNull(values);
    checkArgument(!values.isEmpty());
    this.minValues = new int[values.size()];
    this.maxValues = new int[values.size()];

    TupleParser parser = new VerticalTupleParser();
    for (int i = 0; i < values.size(); i++) {
      String[] range = parser.parse(values.get(i));
      this.minValues[i] = converter.toInt(range[0]);
      this.maxValues[i] = converter.toInt(range[1]);
      checkArgument(this.minValues[i] <= this.maxValues[i]);
    }
    this.min = ArrayUtil.min(this.minValues);
    this.max = ArrayUtil.max(this.maxValues);
  }

  /**
   * Get the global minimum of the conditions
   * 
   * @return the global minimum 
   */
  @Override
  public int getMinKey() {
    return this.min;
  }

  /**
   * Get the global maximum of the conditions
   * 
   * @return the global maximum 
   */
  @Override
  public int getMaxKey() {
    return this.max;
  }

  /**
   * Indicate whether the metafiled is eligible i.e. whether we can find eligible vlaues in the metafield
   * 
   * @param metaField the metafield to be checked
   * @return false indicates the metafield is not eligible and true indicates the metafield is eligible
   */
  @Override
  public boolean accept(MetaFieldRS metaField) {
    return !(metaField.getMinValue() > this.max || metaField.getMaxValue() < this.min);
  }

  /**
   * Indicate whether the filed is eligible i.e. whether we can find eligible vlaues in the field
   * 
   * @param field the field to be checked
   * @return false indicates the field is not eligible and true indicates the field is eligible
   */
  @Override
  public boolean accept(FieldRS field) {
    return !(field.minKey() > this.max || field.maxKey() < this.min);
  }


  /**
   * Indicate whether the interger v is eligible
   * 
   * @param v the interger to be checked
   * @return false indicates the interger is not eligible and true indicates the interger is eligible
   */
  @Override
  public boolean accept(int v) {
    boolean r = false;
    int i = 0;
    while (!r && i < this.minValues.length) {
      r = (v >= this.minValues[i] && v <= this.maxValues[i]);
      i++;
    }
    return r;
  }

  /**
   * Get the conditions set up before
   * 
   * @return the conditions and the minimum and maximum are separated by '|'
   */
  @Override
  public List<String> getValues() {
    List<String> values = Lists.newArrayList();
      for (int i = 0; i < this.minValues.length; i++) {
          values.add(this.minValues[i] + "|" + this.maxValues[i]);
      }
    return values;
  }
}
