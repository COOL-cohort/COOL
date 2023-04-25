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
import java.util.List;

/**
 * Compress data sequences in which same data value occurs in many consecutive
 * data elements are
 * stored as single data value and count.
 * 
 * <p>
 * Data layout
 * ---------------------------------------
 * | zlen | segments | compressed values |
 * ---------------------------------------
 * where segments = number of segment in values
 * 
 * <p>
 * compressed values layout
 * ------------------------
 * | value | offset | len |
 * ------------------------
 */
public class RLECompressor implements Compressor {

  /**
   * Bytes number of z len and number of segments.
   */
  public static final int HEADACC = 4 + 4;

  public RLECompressor() {}

  /**
   * Write int value to a given buffer.
   *
   * @param buf   buffer to be filled
   * @param v     value
   * @param width length of the valuen
   */
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

  /**
   * Add value to a given buffer.
   *
   * @param buf the buffet to filled
   * @param val value
   * @param off offset of the buffer
   * @param len how many the value has been repeated
   */
  private void write(ByteBuffer buf, int val, int off, int len) {
    byte b = 0; // store value's width + offset's width + len's width
    b |= ((IntegerUtil.minBytes(val) << 4) | IntegerUtil.minBytes(off) << 2
        | IntegerUtil.minBytes(len));
    buf.put(b);

    writeInt(buf, val, ((b >> 4) & 3)); // get upper 2 bites
    writeInt(buf, off, ((b >> 2) & 3)); // get middle 2 bites
    writeInt(buf, len, ((b) & 3)); // get lower 2 bites
  }

  @Override
  public CompressorOutput compress(List<? extends FieldValue> src) {
    int maxCompressedLen = HEADACC + 3 * Integer.BYTES * src.size();
    CompressorOutput out = new CompressorOutput(maxCompressedLen);
    ByteBuffer buf = ByteBuffer.wrap(out.getBuf());
    buf.position(HEADACC);
    int nextV = src.get(0).getInt(); // get a different value
    int currOff = 0;
    int voff = currOff;
    int vlen = 0;
    int n = 1; // how many distinct value
    // for each record,
    for (FieldValue v : src) {
      if (v.getInt() != nextV) {
        write(buf, nextV, voff, vlen);
        nextV = v.getInt();
        // re-init offset in output buffer, and length of distinct value
        vlen = 0;
        voff = currOff;
        n++;
      }
      vlen++;
      currOff++;
    }
    // write the last value.
    write(buf, nextV, voff, vlen);
    int zlen = buf.position() - HEADACC;
    buf.position(0);
    buf.putInt(zlen).putInt(n);
    out.setLen(zlen + HEADACC);
    return out;
  }
}
