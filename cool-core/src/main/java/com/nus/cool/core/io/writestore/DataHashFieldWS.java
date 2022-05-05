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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.nus.cool.core.io.DataInputBuffer;
import com.nus.cool.core.io.DataOutputBuffer;
import com.nus.cool.core.io.compression.Histogram;
import com.nus.cool.core.io.compression.OutputCompressor;
import com.nus.cool.core.io.compression.SimpleBitSetCompressor;
import com.nus.cool.core.schema.Codec;
import com.nus.cool.core.schema.CompressType;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.util.ArrayUtil;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * Hash-like indexed field, used to store chunk data for four fieldTypes,
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

  /**
   * Field index to get data from tuple
   */
  private final int fieldIndex;

  private final MetaFieldWS metaField;

  private final OutputCompressor compressor;

  private final FieldType fieldType;

  /**
   * Convert globalID to localID
   * Key: globalID
   * Value: localID
   */
  private final Map<Integer, Integer> idMap = Maps.newTreeMap();

  private final DataOutputBuffer buffer = new DataOutputBuffer();

  private final List<BitSet> bitSetList = Lists.newArrayList();

  private final Boolean preCal;

  public DataHashFieldWS(FieldType fieldType, int fieldIndex, MetaFieldWS metaField, OutputCompressor compressor,
                         boolean preCal) {
    checkArgument(fieldIndex >= 0);
    this.fieldType = fieldType;
    this.fieldIndex = fieldIndex;
    this.metaField = checkNotNull(metaField);
    this.compressor = checkNotNull(compressor);
    this.preCal = preCal;
  }

  @Override
  public FieldType getFieldType() {
    return this.fieldType;
  }

  @Override
  public void put(String tupleValue) throws IOException {
    int gId = this.metaField.find(tupleValue);
      if (gId == -1)
      // The data may be corrupted
      {
          throw new IllegalArgumentException("Value not exist in dimension: " + tupleValue);
      }
    // Write globalIDs as values for temporary
    this.buffer.writeInt(gId);
    // Set localID as 0 for temporary
    // It will be changed while calling writeTo function
    this.idMap.put(gId, 0);
  }

  @Override
  public int writeTo(DataOutput out) throws IOException {
    int bytesWritten = 0;
    // number of global id
    int size = this.buffer.size() / Ints.BYTES;

    // Store globalID in order, key: unique global id
    int[] key = new int[this.idMap.size()];
    // i: local id
    int i = 0;
    for (Map.Entry<Integer, Integer> en : this.idMap.entrySet()) {
      key[i] = en.getKey();
      // Store localID into the map
      en.setValue(i);
      // Store value bitSet if pre-calculate
      if (this.preCal) {
        BitSet bitSet = new BitSet(size);
        this.bitSetList.add(bitSet);
      }
      i++;
    }

    // Store value vector
    int[] value = new int[size];
    // outputBuffer to InputBuffer, for read
    try (DataInputBuffer input = new DataInputBuffer()) {
      input.reset(this.buffer);
      for (i = 0; i < size; i++) {
        int id = input.readInt();
        // Store value as localID
        value[i] = this.idMap.get(id);
        if (this.preCal) {
          // Store value bitSet
          this.bitSetList.get(this.idMap.get(id)).set(i);
        }
      }
    }

    // Write compressed key vector (unique global id)
    int min = ArrayUtil.min(key);
    int max = ArrayUtil.max(key);
    int count = key.length;
    int rawSize = count * Ints.BYTES;
    Histogram hist = Histogram.builder()
        .min(min)
        .max(max)
        .numOfValues(count)
        .rawSize(rawSize)
        .type(CompressType.KeyHash)
        .build();
    this.compressor.reset(hist, key, 0, key.length);
    bytesWritten += this.compressor.writeTo(out);

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
      min = ArrayUtil.min(value);
      max = ArrayUtil.max(value);
      count = value.length;
      rawSize = count * Ints.BYTES;
      hist = Histogram.builder()
          .sorted(this.fieldType == FieldType.AppKey || this.fieldType == FieldType.UserKey)
          .min(min)
          .max(max)
          .numOfValues(count)
          .rawSize(rawSize)
          .type(CompressType.Value)
          .build();
      this.compressor.reset(hist, value, 0, value.length);
      bytesWritten += this.compressor.writeTo(out);
    }
    return bytesWritten;
  }
}
