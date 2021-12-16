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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Compress integers using the leading zero suppressed schema. Each compressed integer is formatted
 * to align to byte boundary and stored in native byte order
 * <p>
 * The data layout is as follows
 * ------------------------------------
 * | count | ZInt compressed integers |
 * ------------------------------------
 *
 * @author zhongle
 * @version 0.1
 * @since 0.1
 */
public class ZIntCompressor implements Compressor {

  /**
   * Bytes number for count
   */
  public static final int HEADACC = 4;

  /**
   * Bytes number for compressed integer
   */
  private int width;

  /**
   * Maximum size of compressed data
   */
  private int maxCompressedLength;

  public ZIntCompressor(Codec codec, Histogram hist) {
    switch (codec) {
      case INT8:
        this.width = 1;
        break;
      case INT16:
        this.width = 2;
        break;
      case INT32:
        this.width = 4;
        break;
      default:
        throw new IllegalArgumentException("Unsupported codec: " + codec);
    }
    this.maxCompressedLength = this.width * hist.getNumOfValues() + HEADACC;
  }

  @Override
  public int maxCompressedLength() {
    return this.maxCompressedLength;
  }

  @Override
  public int compress(byte[] src, int srcOff, int srcLen, byte[] dest, int destOff,
      int maxDestLen) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int compress(int[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen) {
    ByteBuffer buffer = ByteBuffer.wrap(dest, destOff, maxDestLen);
    buffer.order(ByteOrder.nativeOrder());
    // Write count, save srcLen for decompressing
    buffer.putInt(srcLen);
    // Write compressed data
    for (int i = srcOff; i < srcOff + srcLen; i++) {
      switch (this.width) {
        case 1:
          buffer.put((byte) src[i]);
          break;
        case 2:
          buffer.putShort((short) src[i]);
          break;
        case 4:
          buffer.putInt(src[i]);
          break;
      }
    }
    return buffer.position() - destOff;
  }

}
