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

package com.nus.cool.core.io.compression;

import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.util.IntegerUtil;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;

/**
 * Compress a list of integers with BitVector encoding. The final BitSet is
 * encoded in native byte
 * order format.
 * 
 * <p>
 * Data layout
 * ---------------
 * | len | words |
 * ---------------
 * where len is the number of words. Each word is a 64bit computer word
 */
public class BitVectorCompressor implements Compressor {

  /**
   * Bit vector.
   */
  private BitSet bitSet;

  /**
   * Maximum size of compressed data.
   */
  private int maxLength;

  /**
   * Compression operator for bit vector.
   */
  public BitVectorCompressor(FieldValue max) {
    int bitLength = IntegerUtil.numOfBits(max.getInt());
    this.bitSet = new BitSet(bitLength);
    this.maxLength = (bitLength >>> 3) + 1;
  }

  @Override
  public CompressorOutput compress(List<? extends FieldValue> src) {
    // serialize
    for (FieldValue v : src) {
      this.bitSet.set(v.getInt());
    }
    long[] words = this.bitSet.toLongArray();
    byte[] compressed = new byte[this.maxLength];
    ByteBuffer buffer = ByteBuffer.wrap(compressed);
    buffer.put((byte) words.length);
    for (long w : words) {
      buffer.putLong(w);
    }
    // compress
    return new CompressorOutput(compressed, buffer.position());
  }
}
