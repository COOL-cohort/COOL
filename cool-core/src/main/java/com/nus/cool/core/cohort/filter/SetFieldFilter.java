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

import static com.google.common.base.Preconditions.checkNotNull;

import com.nus.cool.core.cohort.ExtendedFieldSet;
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
  private int[] contentIDs;

  /**
   * Indicate which tuple in the table is eligible
   */
  private BitSet filter;

  /**
   * Chunk value vector for hash field
   */
  private InputVector chunkValues;

  private ExtendedFieldSet fieldSet;

  public SetFieldFilter(ExtendedFieldSet set, List<String> values) {
    this.fieldSet = set;
    this.values = checkNotNull(values);
    this.isAll = this.values.contains("ALL");
    this.contentIDs = this.isAll ? new int[2] : new int[values.size()];
  }

  /**
   * Get the minimum cube id of the field
   * 
   * @return the minimum cube id
   */
  @Override
  public int getMinKey() {
    return ArrayUtil.min(this.contentIDs);
  }

  /**
   * Get the maximum cube id of the field
   * 
   * @return the maximum cube id
   */
  @Override
  public int getMaxKey() {
    return ArrayUtil.max(this.contentIDs);
  }

  /**
   * Indicate whether the metafield is eligible i.e. whether we can find eligible values in the metafield
   * 
   * @param metaField the metafield to be checked
   * @return false indicates the metafield is not eligible and true indicates the metafield is eligible
   */
  @Override
  public boolean accept(MetaFieldRS metaField) {
    if (this.isAll) {
      this.contentIDs[1] = metaField.count() - 1;
      return true;
    }
    boolean bHit = false;
    int i = 0;
    // Set up the contentIDs that are the selected conditions
    for (String v : this.values) {
      int tmp = metaField.find(v);
      contentIDs[i++] = tmp;
      bHit |= (tmp >= 0);
    }
    return bHit || (this.values.isEmpty());
  }

  /**
   * Indicate whether the filed is eligible i.e. whether we can find eligible values in the field
   * 
   * @param field the field to be checked
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
    // build a hitset for the filters to check records
    for (int contentID : this.contentIDs) {
      if (contentID >= 0) {
        int tmp = keyVec.find(contentID);
        bHit |= (tmp >= 0);
          if (tmp >= 0) {
              this.filter.set(tmp);
          }
      }
    }
    return bHit || (this.values.isEmpty());
  }

  /**
   * Indicate whether the integer is eligible
   * 
   * @param v the integer to be checked
   * @return false indicates the integer is not eligible and true indicates the integer is eligible
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

  @Override
  public ExtendedFieldSet getFieldSet() {
    return this.fieldSet;
  }

  @Override
  public void updateValues(Double v) {
    this.filter.clear();
    this.filter.set(v.intValue());
  }

  @Override
  public int nextAcceptTuple(int start, int to) {
    chunkValues.skipTo(start);
    while(start < to && !accept(chunkValues.next())) ++start;
    return start;
  }

}
