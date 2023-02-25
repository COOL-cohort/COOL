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

package com.nus.cool.core.io.writestore;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Maps;
import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.HashField;
import com.nus.cool.core.field.ValueWrapper;
import com.nus.cool.core.io.compression.Histogram;
import com.nus.cool.core.io.compression.OutputCompressor;
import com.nus.cool.core.io.compression.SimpleBitSetCompressor;
import com.nus.cool.core.schema.Codec;
import com.nus.cool.core.schema.CompressType;
import com.nus.cool.core.schema.FieldType;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Hash-like indexed field, used to store chunk data for four fieldTypes.
 * including AppKey, UserKey,
 * Action, Segment
 * <p>
 * Data Layout
 * -----------------
 * | keys | values |
 * -----------------
 * where
 * keys = globalIDs
 * (compressed) values = column data, stored as localIDs (compressed)
 */
public class DataHashFieldWS implements DataFieldWS {

  private final MetaHashFieldWS metaField;

  private final FieldType fieldType;

  /**
   * Convert globalID to localID.
   * Key: globalID
   * Value: localID
   */
  private final Map<Integer, Integer> idMap = Maps.newTreeMap();

  // buffer used to store global ID
  private final List<Integer> gIdList = new LinkedList<>();

  private final List<BitSet> bitSetList = new ArrayList<>();

  private final Boolean preCal;

  /**
   * Construct a write store of string fields for data chunk.
   *
   * @param preCal is preCalculated
   */
  public DataHashFieldWS(FieldType fieldType, MetaHashFieldWS metaField, boolean preCal) {
    this.fieldType = fieldType;
    this.metaField = checkNotNull(metaField);
    this.preCal = preCal;
  }

  @Override
  public FieldType getFieldType() {
    return this.fieldType;
  }

  @Override
  public void put(FieldValue tupleValue) throws IllegalArgumentException {
    if (!(tupleValue instanceof HashField)) {
      throw new IllegalArgumentException(
          "Invalid argument for DataHashFieldWS (HashField required)");
    }
    HashField v = (HashField) tupleValue;
    final int gId = this.metaField.find(v);
    if (gId == -1) {
      throw new IllegalArgumentException("Value not exist in dimension: " + v.getString());
    }
    // Write globalIDs as values for temporary
    this.gIdList.add(gId);
    // Set localID as 0 for temporary
    // It will be changed while calling writeTo function
    this.idMap.put(gId, 0);
  }

  @Override
  public int writeTo(DataOutput out) throws IOException {
    // number of global id
    int size = this.gIdList.size();

    // Store globalID in order, key: unique global id
    List<FieldValue> key = new ArrayList<>(this.idMap.size());
    // i: local id, indicate order or globalID
    int i = 0;
    int min = Integer.MAX_VALUE;
    int max = Integer.MIN_VALUE;
    for (Map.Entry<Integer, Integer> en : this.idMap.entrySet()) {
      int k = en.getKey();
      min = (min > k) ? k : min;
      max = (max < k) ? k : max;
      key.add(ValueWrapper.of(k));
      // Store localID into the map
      en.setValue(i);
      // Store value bitSet if pre-calculate
      if (this.preCal) {
        BitSet bitSet = new BitSet(size);
        this.bitSetList.add(bitSet);
      }
      i++;
    }

    // Store value vector, local IDs
    List<FieldValue> value = new ArrayList<>(size);
    i = 0;
    for (Integer id : gIdList) {
      // Store localID to value i-th position
      value.add(ValueWrapper.of(this.idMap.get(id)));
      if (this.preCal) {
        // Store value bitSet
        this.bitSetList.get(this.idMap.get(id)).set(i++);
      }
    }

    // Write compressed key vector (unique global id)
    Histogram hist = Histogram.builder()
        .sorted(true)
        .min(ValueWrapper.of(min))
        .max(ValueWrapper.of(max))
        .numOfValues(key.size())
        .build();
    int bytesWritten = OutputCompressor.writeTo(CompressType.KeyHash, hist, key, out);

    // Write compressed bitSetList if pre-calculate
    if (this.preCal) {
      // Write codec
      out.write(Codec.PreCAL.ordinal());
      bytesWritten++;
      // length of bitSetList should less than a value(i.e. 128), or may throw
      // OOM error and compressed file size would be oversize.
      out.write(this.bitSetList.size());
      bytesWritten++;
      for (BitSet bitSet : this.bitSetList) {
        bytesWritten += SimpleBitSetCompressor.compress(bitSet, out);
      }
    } else {
      // Write compressed value vector (local ids)
      hist = Histogram.builder()
          .min(ValueWrapper.of(0))
          .max(ValueWrapper.of(idMap.size()))
          .numOfValues(value.size())
          .build();
      bytesWritten += OutputCompressor.writeTo(CompressType.Value, hist, value, out);
    }
    return bytesWritten;
  }
}
