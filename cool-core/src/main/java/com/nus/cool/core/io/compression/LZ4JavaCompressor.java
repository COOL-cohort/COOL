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
import com.nus.cool.core.io.DataOutputBuffer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

/**
 * LZ4Compressor for compressing string values.
 * 
 * <p>
 * The compressed data layout
 * ---------------------------------------
 * | z len | raw len | compressed values |
 * ---------------------------------------
 */
public class LZ4JavaCompressor implements Compressor {

  /**
   * Bytes number for z len and raw len.
   */
  public static final int HEADACC = 4 + 4;

  public final Charset charset;

  /**
   * LZ4 compressor.
   */
  private final LZ4Compressor lz4;

  /**
   * Compression operator for general string.
   */
  public LZ4JavaCompressor(Charset charset) {
    this.charset = charset;
    this.lz4 = LZ4Factory.fastestInstance().fastCompressor();
  }
  
  @Override
  public CompressorOutput compress(List<? extends FieldValue> src) {
    try {
      // serialize
      DataOutputBuffer srcBuf = new DataOutputBuffer();
      srcBuf.writeInt(src.size());

      int offset = 0;
      for (FieldValue v : src) { 
        srcBuf.writeInt(offset);
        offset += v.getString().getBytes(this.charset).length;
      }

      for (FieldValue v : src) { 
        srcBuf.write(v.getString().getBytes(this.charset));
      }
      // compress
      int maxLen = lz4.maxCompressedLength(srcBuf.size()) + HEADACC;
      byte[] compressed = new byte[maxLen];
      int zlen = this.lz4.compress(srcBuf.getData(), 0, srcBuf.size(), compressed,
          HEADACC, maxLen - HEADACC);
      ByteBuffer buffer = ByteBuffer.wrap(compressed, 0, HEADACC);
      // write z len and raw len for decompressing
      buffer.putInt(zlen);
      buffer.putInt(srcBuf.size());
      srcBuf.close();
      return new CompressorOutput(compressed, HEADACC + zlen);
    } catch (IOException e) {
      System.out.println("IO exception while serializing field values");
      return CompressorOutput.emptyCompressorOutput();
    }
  }
}
