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

import com.nus.cool.core.util.IntegerUtil;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Compress data sequences in which same data value occurs in many consecutive data elements are
 * stored as single data value and count.
 * <p>
 * Data layout
 * ---------------------------------------
 * | zLen | segments | compressed values |
 * ---------------------------------------
 * where segments = number of segment in values
 * <p>
 * compressed values layout
 * ------------------------
 * | value | offset | len |
 * ------------------------
 */
public class RLECompressor implements Compressor {

  /**
   * Bytes number of z len and number of segments
   */
  public static final int HEADACC = 4 + 4;

  private final int maxCompressedLen;

  public RLECompressor(Histogram hist) {
    int uncompressedSize = 3 * Integer.BYTES * hist.getNumOfValues();
    this.maxCompressedLen = HEADACC + (Math.max(hist.getRawSize(), uncompressedSize));
  }

  private void writeInt(ByteBuffer buf, int v, int width) {
    switch (width) {
      case 1:
        buf.put((byte) v);
        break;
      case 2:
        buf.putShort((short) v);
        break;
      case 3:
      case 0:
        buf.putInt(v);
        break;
      default:
        throw new java.lang.IllegalArgumentException("incorrect number of bytes");
    }
  }

  private void write(ByteBuffer buf, int val, int off, int len) {
    byte b = 0;
    b |= ((IntegerUtil.minBytes(val) << 4) | IntegerUtil.minBytes(off) << 2 | IntegerUtil
        .minBytes(len));
    buf.put(b);

    writeInt(buf, val, ((b >> 4) & 3));
     writeInt(buf, off, ((b >> 2) & 3));
    writeInt(buf, len, ((b) & 3));
  }

  @Override
  public int maxCompressedLength() {
    return this.maxCompressedLen;
  }

  @Override
  public int compress(byte[] src, int srcOff, int srcLen, byte[] dest,
      int destOff, int maxDestLen) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int compress(int[] src, int srcOff, int srcLen, byte[] dest,
      int destOff, int maxDestLen) {
    int n = 1;
    ByteBuffer buf = ByteBuffer.wrap(dest, destOff, maxDestLen).order(ByteOrder.nativeOrder());
    buf.position(HEADACC);
    int v = src[srcOff], voff = 0, vlen = 1;
    for (int i = srcOff + 1; i < srcOff + srcLen; i++) {
      if (src[i] != v) {
        write(buf, v, voff, vlen);
        v = src[i];
        voff = i - srcOff;
        vlen = 1;
        n++;
      } else {
        vlen++;
      }
    }
    write(buf, v, voff, vlen);
    int zLen = buf.position() - HEADACC;
    buf.position(0);
    buf.putInt(zLen).putInt(n);
    return zLen + HEADACC;
  }

}
