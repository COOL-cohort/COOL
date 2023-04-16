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

/**
 * Product compressor according to codes.
 */
public class CompressorFactory {

  /**
   * Create a compressor.
   */ 
  public static Compressor newCompressor(Codec codec, Histogram hist) {
    Compressor compressor;
    switch (codec) {
      case INT8:
      case INT16:
      case INT32:
        compressor = new ZIntCompressor(codec, hist.isSorted());
        break;
      case LZ4:
        compressor = new LZ4JavaCompressor(hist.getCharset());
        break;
      case BitVector:
        compressor = new BitVectorCompressor(hist.getMax());
        break;
      case RLE:
        compressor = new RLECompressor();
        break;
      case INTBit:
        compressor = new ZIntBitCompressor(hist.getMax());
        break;
      case Delta:
        compressor = new DeltaCompressor(hist.getMin(), hist.getMax());
        break;
      case Float:
        compressor = new FloatCompressor();
        break;
      default:
        throw new IllegalArgumentException("Unsupported codec: " + codec);
    }
    return compressor;
  }
}
