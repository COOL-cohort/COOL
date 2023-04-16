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

import com.nus.cool.core.schema.Codec;
import com.nus.cool.core.schema.CompressType;
import com.nus.cool.core.util.IntegerUtil;

/**
 * Advise codec according to compress type.
 * 
 * <p>
 * KeyFinger -> LZ4
 * KeyString -> INT32
 * KeyHash -> { BitVector, INT8, INT16, INT32 }
 * Value -> { RLE, INT8, INT16, INT32, INTBit }
 */
public class CompressorAdviser {

  /**
   * Advise the type of compressor to use.
   */
  public static Codec advise(CompressType type, Histogram hist) {
    switch (type) {
      case KeyFinger:
        return Codec.INT32;
      case KeyString:
        return Codec.LZ4;
      case KeyHash:
        return adviseForKeyHash(hist);
      case Value:
        return adviseForValue(hist);
      case ValueFast:
        return Codec.Delta;
      case Float:
        return Codec.Float;
      default:
        throw new IllegalArgumentException("Unsupported compress type: " + type);
    }
  }

  // This assumes the values in the bytes buffer stored to be in ascending order
  // because the use of bitvector will implicitly sort the data during read
  // the value retrieved from small to big to populate a vector.
  private static Codec adviseForKeyHash(Histogram hist) {
    // max value determine which numeric type is better
    // including INT8, INT16, INT32.
    int max = hist.getMax().getInt();

    // Get bitSet size to represent n values, n equals max
    int bitmapLength = IntegerUtil.numOfBits(max);
    int bitmapBytes = ((bitmapLength - 1) >>> 3) + 1;

    int bytes = IntegerUtil.minBytes(max);
    // byte size
    int byteAlignLength = bytes * hist.getNumOfValues();

    // max < 2^8
    if (max < 256) {
      // TODO: Optimize this condition for better performance with INT16 and INT32
      // If byte size >= bitSet size, bitVector is a better choice
      if (bitmapBytes <= byteAlignLength) {
        return Codec.BitVector;
      } else {
        return Codec.INT8;
      }
    } else if (max < 65536) {
      // max < 2^16
      return Codec.INT16;
    } else {
      // max < 2^32 (Integer.MAX_VALUE)
      return Codec.INT32;
    }
  }

  // TODO: NEED docs
  // this does not implicit assume the values are being sorted.
  // RLE and INtBit readstore does not support find.
  private static Codec adviseForValue(Histogram hist) {
    
    // if (hist.isSorted()) {
    //   return Codec.RLE;
    // }

    int max = hist.getMax().getInt();
    int bitLength = IntegerUtil.minBits(max);
    int byteLength = IntegerUtil.minBytes(max);
    if (bitLength / (byteLength * 8.0) >= 0.7) {
      switch (byteLength) {
        case 1:
          return Codec.INT8;
        case 2:
          return Codec.INT16;
        case 4:
          return Codec.INT32;
        default:
          throw new IllegalArgumentException("Unsupported byte length: " + byteLength);
      }
    }
    return Codec.INTBit;
  }

}
