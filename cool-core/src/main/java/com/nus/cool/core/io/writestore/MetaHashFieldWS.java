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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.HashField;
import com.nus.cool.core.field.ValueWrapper;
import com.nus.cool.core.io.compression.Histogram;
import com.nus.cool.core.io.compression.OutputCompressor;
import com.nus.cool.core.schema.CompressType;
import com.nus.cool.core.schema.FieldType;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Hash MetaField write store.
 * <p>
 * Hash MetaField layout
 * ---------------------------------------
 * | finger codec | fingers | value data |
 * ---------------------------------------
 * <p>
 * if fieldType == AppId, we do NOT store values
 * <p>
 * value data layout
 * --------------------------------------------------
 * | #values | value codec | value offsets | values |
 * --------------------------------------------------
 */
public class MetaHashFieldWS implements MetaFieldWS {

  protected final Charset charset;
  protected final FieldType fieldType;

  // hash(value): global id
  protected Map<Integer, Integer> fingerToGid = Maps.newTreeMap();
  protected final List<HashField> valueList = new ArrayList<>();

  // all possible values of the field.
  protected final Set<FieldValue> fieldValues = Sets.newTreeSet();

  // global id
  protected int nextGid = 0;

  /**
   * Constructor of MetaHashFieldWS.
   *
   * @param type       type
   * @param charset    charset
   */
  public MetaHashFieldWS(FieldType type, Charset charset) {
    this.fieldType = type;
    this.charset = charset;
  }

  @Override
  public void put(FieldValue[] tuple, int idx) throws IllegalArgumentException {
    // int hashKey = rhash.hash(tuple[idx]);
    if (!(tuple[idx] instanceof HashField)) {
      throw new IllegalArgumentException(
        "Invalid argument for MetaHashFieldWS (HashField required).");
    }
    HashField v = (HashField) tuple[idx];
    int hashKey = v.getInt();
    if (!this.fingerToGid.containsKey(hashKey)) {
      this.fingerToGid.put(hashKey, nextGid++);
      this.valueList.add(v);
    }
  }

  /**
   * Find the index of value in this meta field, return -1 if no such value
   * exists.
   *
   * @param v target value
   * @return index of value in this meta field
   */
  public int find(HashField v) {
    // int fp = this.rhash.hash(v);
    int hashKey = v.getInt();
    return this.fingerToGid.containsKey(hashKey) ? this.fingerToGid.get(hashKey) : -1;
  }

  @Override
  public int count() {
    return this.valueList.size();
  }

  @Override
  public FieldType getFieldType() {
    return this.fieldType;
  }

  @Override
  public void complete() {
    for (FieldValue v : this.valueList) {
      fieldValues.add(v);
    }
  }

  @Override
  public void cleanForNextCublet() {
    this.fingerToGid.clear();
    this.valueList.clear();
    this.nextGid = 0; // a field can have different id across cublet.
  }

  protected int writeFingersAndGids(DataOutput out) throws IOException {
    int bytesWritten = 0;
    // Write fingers, i.e., the hash values of the original string, into the array
    // fingers contain data's hash value
    List<FieldValue> fingers = new ArrayList<>(this.fingerToGid.size());
    // globalIDs contain the global ids in the hash order
    List<FieldValue> globalIDs = new ArrayList<>(this.fingerToGid.size());

    for (Map.Entry<Integer, Integer> en : this.fingerToGid.entrySet()) {
      globalIDs.add(ValueWrapper.of(en.getValue()));
      fingers.add(ValueWrapper.of(en.getKey()));
    }

    // generate finger bytes
    Histogram hist = Histogram.builder()
        .sorted(true)
        .min(fingers.get(0))
        .max(fingers.get(fingers.size() - 1))
        .numOfValues(fingers.size())
        .build();
    // Compress and write the fingers
    // Codec is written internal
    bytesWritten += OutputCompressor.writeTo(CompressType.KeyFinger, hist, fingers, out);

    // generate globalID bytes
    hist = Histogram.builder()
        .min(ValueWrapper.of(0))
        .max(ValueWrapper.of(this.fingerToGid.size()))
        .numOfValues(globalIDs.size())
        .build();
    bytesWritten += OutputCompressor.writeTo(CompressType.Value, hist, globalIDs, out);
    return bytesWritten;
  }

  @Override
  public int writeTo(DataOutput out) throws IOException {
    int bytesWritten = writeFingersAndGids(out);
    bytesWritten += OutputCompressor.writeTo(CompressType.KeyString,
      Histogram.builder().charset(charset).build(),
      valueList, out);

    return bytesWritten;
  }

  @Override
  public int writeCubeMeta(DataOutput out) throws IOException {
    if (this.fieldType != FieldType.Segment
        && this.fieldType != FieldType.Action
        && this.fieldType != FieldType.UserKey) {
      return 0;
    }
    return OutputCompressor.writeTo(CompressType.KeyString,
      Histogram.builder().charset(charset).build(),
      new ArrayList<FieldValue>(fieldValues), out);
  }

  @Override
  public String toString() {
    return "HashMetaField: " + valueList.toString();
  }
}
