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
package com.nus.cool.core.io.compression;

import com.nus.cool.core.schema.Codec;

/**
 * Product compressor according to codes
 *
 * @author zhongle
 * @version 0.1
 * @since 0.1
 */
public class CompressorFactory {

  public static Compressor newCompressor(Codec codec, Histogram hist) {
    Compressor compressor;
    switch (codec) {
      case INT8:
      case INT16:
      case INT32:
        compressor = new ZIntCompressor(codec, hist);
        break;
      case LZ4:
        compressor = new LZ4JavaCompressor(hist);
        break;
      case BitVector:
        compressor = new BitVectorCompressor(hist);
        break;
      case RLE:
        compressor = new RLECompressor(hist);
        break;
      case INTBit:
        compressor = new ZIntBitCompressor(hist);
        break;
      case Delta:
        compressor = new DeltaCompressor(hist);
        break;
      default:
        throw new IllegalArgumentException("Unsupported codec: " + codec);
    }
    return compressor;
  }
}
