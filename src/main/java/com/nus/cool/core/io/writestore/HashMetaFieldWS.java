/*
 * Copyright 2020 Cool Squad Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nus.cool.core.io.writestore;

import com.google.common.collect.Maps;
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
import java.util.Map;

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
 *
 * @author zhongle
 * @version 0.1
 * @since 0.1
 */
public class HashMetaFieldWS implements MetaFieldWS {

  private Charset charset;
  private FieldType fieldType;
  private OutputCompressor compressor;
  private RabinHashFunction32 rhash = RabinHashFunction32.DEFAULT_HASH_FUNCTION;

  /**
   * Global dictionary, keys are hashed by the indexed string
   */
  private Map<Integer, Term> dict = Maps.newTreeMap();

  public HashMetaFieldWS(FieldType type, Charset charset, OutputCompressor compressor) {
    this.fieldType = type;
    this.charset = charset;
    this.compressor = compressor;
  }

  @Override
  public void put(String v) {
    // Set globalID as 0 for temporary
    // It will changed after calling complete function
    this.dict.put(rhash.hash(v), new Term(v, 0));
  }

  @Override
  public int find(String v) {
    // TODO: Need to handle the case where v is null
    int fp = this.rhash.hash(v);
    return this.dict.containsKey(fp) ? this.dict.get(fp).globalId : -1;
  }

  @Override
  public int count() {
    return this.dict.size();
  }

  @Override
  public FieldType getFieldType() {
    return this.fieldType;
  }

  @Override
  public void complete() {
    int gID = 0;
    // Set globalIDs
    for (Map.Entry<Integer, Term> en : this.dict.entrySet()) {
      en.getValue().globalId = gID++;
    }
  }

  @Override
  public int writeTo(DataOutput out) throws IOException {
    int bytesWritten = 0;

    // Write fingers, i.e., the hash values of the original string, into the array
    // fingers contain data's hash value
    int[] fingers = new int[this.dict.size()];
    int i = 0;
    for (Map.Entry<Integer, Term> en : this.dict.entrySet()) {
      fingers[i++] = en.getKey();
    }
    Histogram hist = Histogram.builder()
        .min(fingers[0])
        .max(fingers[fingers.length - 1])
        .numOfValues(fingers.length)
        .rawSize(Ints.BYTES * fingers.length)
        .type(CompressType.KeyFinger)
        .build();
    this.compressor.reset(hist, fingers, 0, fingers.length);
    // Compress and write the fingers
    // Codec is written internal
    // Actually data is not compressed here
    bytesWritten += this.compressor.writeTo(out);

    // Write values
    if (this.fieldType == FieldType.Segment || this.fieldType == FieldType.Action
        || this.fieldType == FieldType.UserKey) {
      try (DataOutputBuffer buffer = new DataOutputBuffer()) {
        // Store offsets into the buffer first
        buffer.writeInt(this.dict.size());
        // Value offsets begin with 0
        int off = 0;
        for (Map.Entry<Integer, Term> en : this.dict.entrySet()) {
          buffer.writeInt(off);
          off += en.getValue().term.getBytes(this.charset).length;
        }

        //Store String values into the buffer
        for (Map.Entry<Integer, Term> en : this.dict.entrySet()) {
          buffer.write(en.getValue().term.getBytes(this.charset));
        }

        // Compress and write the buffer
        // The codec is written internal
        hist = Histogram.builder()
            .type(CompressType.KeyString)
            .rawSize(buffer.size())
            .build();
        this.compressor.reset(hist, buffer.getData(), 0, buffer.size());
        bytesWritten += this.compressor.writeTo(out);
      }
    }
    return bytesWritten;
  }

  /**
   * Convert string to globalIDs
   */
  public static class Term {

    String term;
    int globalId;

    public Term(String term, int globalId) {
      this.term = term;
      this.globalId = globalId;
    }
  }
}