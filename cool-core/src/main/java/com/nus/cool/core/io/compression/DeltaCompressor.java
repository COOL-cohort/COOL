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

import com.google.common.primitives.Ints;
import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.ValueWrapper;
import com.nus.cool.core.schema.Codec;
import com.nus.cool.core.util.IntegerUtil;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Compress integers using delta encoding, get max delta by max value and min
 * value in integers
 * and choose ZIntCompressor by max delta and compress delta value vector by
 * ZIntCompressor.
 * 
 * <p>
 * Data layout is as follows
 * ------------------------------------------------------------------------------------------------
 * | min | max | zInt Codec | zInt compressed values |
 * ------------------------------------------------------------------------------------------------
 */
public class DeltaCompressor implements Compressor {

  /**
   * Head account for min, max and zInt codec.
   */
  public static final int HEADACC = 8 + 1;

  // private int numOfVal;

  private FieldValue min;

  private FieldValue max;

  public DeltaCompressor(FieldValue min, FieldValue max) {
    this.min = min;
    this.max = max;
  }

  // @Override
  private int maxCompressedLength(int count) {
    return  count * Ints.BYTES + ZIntCompressor.HEADACC + HEADACC;
  }

  @Override
  public CompressorOutput compress(List<? extends FieldValue> src) {
    int minValue = min.getInt();
    int maxValue = max.getInt();
    byte[] buf = new byte[maxCompressedLength(src.size())];
    ByteBuffer buffer = ByteBuffer.wrap(buf);
    buffer.putInt(minValue);
    buffer.putInt(maxValue);

    // prepare delta data for compression
    List<FieldValue> valuesToCompress = src.stream()
        .map(x -> ValueWrapper.of(x.getInt() - minValue))
        .collect(Collectors.toList());

    // Get ZIntCompressor by max delta
    int maxDelta = maxValue - minValue;
    Codec codec = getCodec(maxDelta);
    // Write codec for ZIntCompressor type, i.e. INT8, INT16, INT32
    buffer.put((byte) codec.ordinal());
    ZIntCompressor compressor = new ZIntCompressor(codec, false);
    int offset = HEADACC;
    int compressedDeltaLen = compressor.compress(valuesToCompress,
        buf, offset, buf.length - offset);
    return new CompressorOutput(buf, HEADACC + compressedDeltaLen);
  }

  private Codec getCodec(int v) {
    int width = IntegerUtil.minBytes(v);
    switch (width) {
      case 1:
        return Codec.INT8;
      case 2:
        return Codec.INT16;
      case 4:
        return Codec.INT32;
      default:
        throw new IllegalArgumentException("Unsupported width: " + width);
    }
  }
}
