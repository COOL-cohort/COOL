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
import com.nus.cool.core.schema.Codec;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Compress integers using the leading zero suppressed schema. Each compressed integer is formatted
 * to align to byte boundary and stored in native byte order
 * <p>
 * The data layout is as follows
 * ------------------------------------
 * | count | ZInt compressed integers |
 * ------------------------------------
 */
public class ZIntCompressor implements Compressor {

  /**
   * Bytes number for count and sorted.
   */
  public static final int HEADACC = 5;

  /**
   * Bytes number for compressed integer.
   */
  private final int width;
  
  /**
   *  Whether these value are sorted.
   */
  private final boolean sorted;

  /**
   * Create a ZInt compressor with fixed width.
   */
  public ZIntCompressor(Codec codec, boolean sorted) {
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
    this.sorted = sorted;
  }

  /**
   * Compress into pre-allocated buffer for DeltaCompressor.
   */
  public int compress(List<? extends FieldValue> src, byte[] dest, int destOff,
      int maxDestLen) {
    ByteBuffer buffer = ByteBuffer.wrap(dest, destOff, maxDestLen);
    // Write count, save srcLen for decompressing
    buffer.putInt(src.size());
    byte sorted = this.sorted ? (byte) 1 : (byte) 0;
    buffer.put(sorted);
    // Write compressed data
    for (FieldValue v : src) {
      switch (this.width) {
        case 1:
          buffer.put((byte) v.getInt());
          break;
        case 2:
          buffer.putShort((short) v.getInt());
          break;
        case 4:
          buffer.putInt(v.getInt());
          break;
        default:
          throw new IllegalStateException("Invalid width: " + this.width);
      }
    }
    return buffer.position() - destOff;
  }

  @Override
  public CompressorOutput compress(List<? extends FieldValue> src) {
    CompressorOutput out = new CompressorOutput(this.width * src.size() + HEADACC);
    ByteBuffer buffer = ByteBuffer.wrap(out.getBuf());
    // Write count, save srcLen for decompressing
    buffer.putInt(src.size());
    byte sorted = this.sorted ? (byte) 1 : (byte) 0;
    buffer.put(sorted);
    // Write compressed data
    for (FieldValue v : src) {
      switch (this.width) {
        case 1:
          buffer.put((byte) v.getInt());
          break;
        case 2:
          buffer.putShort((short) v.getInt());
          break;
        case 4:
          buffer.putInt(v.getInt());
          break;
        default:
          throw new IllegalStateException("Invalid width: " + this.width);
      }
    }
    out.setLen(buffer.position());
    return out;
  }
}