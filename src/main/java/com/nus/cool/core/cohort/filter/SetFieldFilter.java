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

import static com.google.common.base.Preconditions.checkNotNull;

import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.util.ArrayUtil;
import java.util.BitSet;
import java.util.List;

/**
 * SetFieldFilter is used to check whether the input is eligible
 * If the condition doesnot exist, then all the input are eligible
 * Usage: first check whether the metafield is eligible
 * Then find the eligible tuples of the field
 */
public class SetFieldFilter implements FieldFilter {

  /**
   * The conditions we set up
   */
  private List<String> values;

  /**
   * whether the condition means all the tuples
   */
  private boolean isAll;

  /**
   * Which condition it contains
   */
  private int[] cubeIDs;

  /**
   * Indicate which tuple in the table is eligible
   */
  private BitSet filter;

  /**
   * Chunk value vector for hash field
   */
  private InputVector chunkValues;

  public SetFieldFilter(List<String> values) {
    this.values = checkNotNull(values);
    this.isAll = this.values.contains("ALL");
    this.cubeIDs = this.isAll ? new int[2] : new int[values.size()];
  }

  /**
   * Get the minimum cube id of the field
   * 
   * @return the minimum cube id
   */
  @Override
  public int getMinKey() {
    return ArrayUtil.min(this.cubeIDs);
  }

  /**
   * Get the maximum cube id of the field
   * 
   * @return the maximum cube id
   */
  @Override
  public int getMaxKey() {
    return ArrayUtil.max(this.cubeIDs);
  }

  /**
   * Indicate whether the metafiled is eligible i.e. whether we can find eligible vlaues in the metafield
   * 
   * @param metaField the metafield to be checked
   * @return false indicates the metafield is not eligible and true indicates the metafield is eligible
   */
  @Override
  public boolean accept(MetaFieldRS metaField) {
    if (this.isAll) {
      this.cubeIDs[1] = metaField.count() - 1;
      return true;
    }
    boolean bHit = false;
    int i = 0;
    for (String v : this.values) {
      int tmp = metaField.find(v);
      cubeIDs[i++] = tmp;
      bHit |= (tmp >= 0);
    }
    return bHit || (this.values.isEmpty());
  }

  /**
   * Indicate whether the filed is eligible i.e. whether we can find eligible vlaues in the field
   * 
   * @param metaField the field to be checked
   * @return false indicates the field is not eligible and true indicates the field is eligible
   */
  @Override
  public boolean accept(FieldRS field) {
      if (this.isAll) {
          return true;
      }

    InputVector keyVec = field.getKeyVector();
    this.filter = new BitSet(keyVec.size());
    this.chunkValues = field.getValueVector();

    boolean bHit = false;
    for (int cubeId : this.cubeIDs) {
      if (cubeId >= 0) {
        int tmp = keyVec.find(cubeId);
        bHit |= (tmp >= 0);
          if (tmp >= 0) {
              this.filter.set(tmp);
          }
      }
    }
    return bHit || (this.values.isEmpty());
  }

  /**
   * Indicate whether the interger is eligible
   * 
   * @param metaField the interger to be checked
   * @return false indicates the interger is not eligible and true indicates the interger is eligible
   */
  @Override
  public boolean accept(int v) {
      if (this.isAll) {
          return true;
      }
    return this.filter.get(v);
  }

  /**
   * Get the conditions set up before
   * 
   * @return the string conditions we set up
   */
  @Override
  public List<String> getValues() {
    return this.values;
  }
}
