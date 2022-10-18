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
import com.google.common.primitives.Ints;
import com.nus.cool.core.io.DataOutputBuffer;
import com.nus.cool.core.io.compression.Histogram;
import com.nus.cool.core.io.compression.OutputCompressor;
import com.nus.cool.core.schema.CompressType;
import com.nus.cool.core.schema.FieldType;
import com.rabinhash.RabinHashFunction32;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Hash MetaField write store
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
  protected final OutputCompressor compressor;
  protected final RabinHashFunction32 rhash = RabinHashFunction32.DEFAULT_HASH_FUNCTION;
  
  protected Map<Integer, Integer> fingerToGid = Maps.newTreeMap();
  protected final List<String> valueList = new ArrayList<>();
  
  /**
   * Global hashToTerm, keys are hashed by the indexed string
   */

  // hash of one tuple field : Term {origin value of tuple filed, global ID. }
  // protected final Map<Integer, Term> hashToTerm = Maps.newTreeMap();

  // all possible values of the field.
  // protected final Set<String> fieldValues = Sets.newTreeSet();

  // global id
  protected int nextGid = 0;

  public MetaHashFieldWS(FieldType type, Charset charset, OutputCompressor compressor) {
    this.fieldType = type;
    this.charset = charset;
    this.compressor = compressor;
  }

  @Override
  public void put(String[] tuple, int idx) {
    int hashKey = rhash.hash(tuple[idx]);
    if (!this.fingerToGid.containsKey(hashKey)){
      this.fingerToGid.put(hashKey, nextGid++);
      this.valueList.add(tuple[idx]);
    }
    // if (!this.hashToTerm.containsKey(hashKey)) {
    //   this.hashToTerm.put(hashKey, new Term(tuple[idx], nextGid++));
    // }
  }

  @Override
  public int find(String v) {
    int fp = this.rhash.hash(v);
    // return this.hashToTerm.containsKey(fp) ? this.hashToTerm.get(fp).globalId : -1;
    return this.fingerToGid.containsKey(fp) ? this.fingerToGid.get(fp) : -1;
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
    // for (Map.Entry<Integer, Term> en : this.hashToTerm.entrySet()) {
    //   fieldValues.add(en.getValue().term);
    // }
  }

  @Override
  public void cleanForNextCublet() {
    this.fingerToGid.clear();
    this.valueList.clear();
    this.nextGid = 0; // a field can have different id across cublet.
  }

  @Override
  public int writeTo(DataOutput out) throws IOException {
    int bytesWritten = 0;

    // Write fingers, i.e., the hash values of the original string, into the array
    // fingers contain data's hash value
    int[] fingers = new int[this.fingerToGid.size()];
    // globalIDs contain the global ids in the hash order
    int[] globalIDs = new int[this.fingerToGid.size()];
    int i = 0;

    for (Map.Entry<Integer, Integer> en : this.fingerToGid.entrySet()) {
      globalIDs[i] = en.getValue();
      fingers[i++] = en.getKey();
    }

    // generate finger bytes
    Histogram hist = Histogram.builder()
        .min(fingers[0])
        .max(fingers[fingers.length - 1])
        .numOfValues(fingers.length)
        .rawSize(Ints.BYTES * fingers.length)
        .type(CompressType.KeyFinger)
        .sorted(true)
        .build();
    this.compressor.reset(hist, fingers, 0, fingers.length);
    // Compress and write the fingers
    // Codec is written internal
    // Actually data is not compressed here
    bytesWritten += this.compressor.writeTo(out);

    // generate globalID bytes
    hist = Histogram.builder()
        .min(0)
        .max(this.fingerToGid.size())
        .numOfValues(globalIDs.length)
        .rawSize(Ints.BYTES * globalIDs.length)
        .type(CompressType.Value)// choose value as it is used for hash field columns of global id.
        .build();
    this.compressor.reset(hist, globalIDs, 0, globalIDs.length);
    bytesWritten += this.compressor.writeTo(out);

    // Write values


    try (DataOutputBuffer buffer = new DataOutputBuffer()) {
      // Store offsets into the buffer first
      buffer.writeInt(this.valueList.size());
      // Value offsets begin with 0
      int off = 0;
      for (String t : this.valueList) {
        buffer.writeInt(off);
        off += t.getBytes(this.charset).length;
      }

      // Store String values into the buffer
      for (String t : this.valueList) {
        buffer.write(t.getBytes(this.charset));
      }
      // for (Map.Entry<Integer, Term> en : this.hashToTerm.entrySet()) {
      // buffer.writeInt(off);
      // off += en.getValue().term.getBytes(this.charset).length;
      // }

      // // Store String values into the buffer
      // for (Map.Entry<Integer, Term> en : this.hashToTerm.entrySet()) {
      // buffer.write(en.getValue().term.getBytes(this.charset));
      // }

      // Compress and write the buffer
      // The codec is written internal
      hist = Histogram.builder()
          .type(CompressType.KeyString)
          .rawSize(buffer.size())
          .build();
      this.compressor.reset(hist, buffer.getData(), 0, buffer.size());
      bytesWritten += this.compressor.writeTo(out);
    }

    return bytesWritten;
  }

  @Override
  public int writeCubeMeta(DataOutput out) throws IOException {
    int bytesWritten = 0;
    if (this.fieldType == FieldType.Segment
        || this.fieldType == FieldType.Action
        || this.fieldType == FieldType.AppKey) {
      try (DataOutputBuffer buffer = new DataOutputBuffer()) {
        buffer.writeInt(this.valueList.size());
        int off = 0;

        for (String s : this.valueList) {
          buffer.writeInt(off);
          off += s.getBytes(this.charset).length;
        }

        for (String s : this.valueList) {
          buffer.write(s.getBytes(this.charset));
        }

        Histogram hist = Histogram.builder()
            .type(CompressType.KeyString)
            .rawSize(buffer.size())
            .build();
        this.compressor.reset(hist, buffer.getData(), 0, buffer.size());
        bytesWritten = this.compressor.writeTo(out);
      }
    }
    return bytesWritten;
  }

  @Override
  public String toString() {
    return "HashMetaField: "
        + valueList.toString();
  }

  /**
   * Convert string to globalIDs
   */
  public static class Term implements Comparable<Term> {

    // the real value in each row of the csv file
    String term;
    // assigned global ID.
    int globalId;

    public Term(String term, int globalId) {
      this.term = term;
      this.globalId = globalId;
    }

    @Override
    public String toString() {
      return "{term: " + term + ", globalId: " + globalId + "}";
    }

    @Override
    public int compareTo(Term t) {
      if (this.globalId < t.globalId)
        return -1;
      else if (this.globalId > t.globalId)
        return 1;
      else
        return 0;
    }
  }
}