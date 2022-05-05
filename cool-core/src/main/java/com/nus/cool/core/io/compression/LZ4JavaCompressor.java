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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

/**
 * LZ4Compressor for compressing string values
 * <p>
 * The compressed data layout
 * ---------------------------------------
 * | z len | raw len | compressed values |
 * ---------------------------------------
 */
public class LZ4JavaCompressor implements Compressor {

  /**
   * Bytes number for z len and raw len
   */
  public static final int HEADACC = 4 + 4;

  /**
   * Maximum size of compressed data
   */
  private final int maxLen;

  /**
   * LZ4 compressor
   */
  private final LZ4Compressor lz4;

  public LZ4JavaCompressor(Histogram hist) {
    this.lz4 = LZ4Factory.fastestInstance().fastCompressor();
    this.maxLen = lz4.maxCompressedLength(hist.getRawSize()) + HEADACC;
  }

  @Override
  public int maxCompressedLength() {
    return this.maxLen;
  }

  @Override
  public int compress(byte[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen) {
    ByteBuffer buffer = ByteBuffer.wrap(dest, destOff, maxDestLen).order(ByteOrder.nativeOrder());
    int zLen = this.lz4.compress(src, srcOff, srcLen, dest, destOff + HEADACC, maxDestLen - HEADACC);
    // write z len and raw len for decompressing
    buffer.putInt(zLen);
    buffer.putInt(srcLen);
    return HEADACC + zLen;
  }

  @Override
  public int compress(int[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen) {
    throw new UnsupportedOperationException();
  }

}
