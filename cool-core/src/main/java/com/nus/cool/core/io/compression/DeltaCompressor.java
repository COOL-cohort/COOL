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
import com.nus.cool.core.schema.Codec;
import com.nus.cool.core.util.IntegerUtil;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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

  private int numOfVal;

  private int minValue;

  private int maxValue;

  public DeltaCompressor(Histogram hist) {
    this.numOfVal = hist.getNumOfValues();
  }

  @Override
  public int maxCompressedLength() {
    return (this.numOfVal * Ints.BYTES) + ZIntCompressor.HEADACC + HEADACC;
  }

  @Override
  public int compress(byte[] src, int srcOff, int srcLen, byte[] dest,
      int destOff, int maxDestLen) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int compress(int[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen) {
    // Get min, max and number of values
    getMetaData(src, srcOff, srcLen);

    // prepare delta data for compression
    int[] valuesToCompress = new int[srcLen];
    for (int i = 0; i < srcLen; i++) {
      valuesToCompress[i] = src[srcOff + i] - minValue;
    }

    ByteBuffer buffer = ByteBuffer.wrap(dest, destOff, maxDestLen);
    buffer.order(ByteOrder.nativeOrder());
    buffer.putInt(minValue);
    buffer.putInt(maxValue);

    // Get ZIntCompressor by max delta
    int maxDelta = maxValue - minValue;
    Histogram hist = Histogram.builder()
        .max(maxDelta)
        .numOfValues(numOfVal)
        .build();
    Codec codec = getCodec(maxDelta);
    // Write codec for ZIntCompressor type, i.e. INT8, INT16, INT32
    buffer.put((byte) codec.ordinal());
    ZIntCompressor compressor = new ZIntCompressor(codec, hist);
    int offset = HEADACC;
    return HEADACC + compressor.compress(valuesToCompress, 0, srcLen,
        dest, destOff + offset, maxDestLen - offset);
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

  private void getMetaData(int[] src, int srcOff, int srcLen) {
    numOfVal = srcLen;
    minValue = src[0];
    maxValue = src[0];

    for (int i = 1; i < srcLen; i++) {
      int cur = src[i + srcOff];
      if (minValue > cur) {
        minValue = cur;
      }
      if (maxValue < cur) {
        maxValue = cur;
      }
    }
  }
}
