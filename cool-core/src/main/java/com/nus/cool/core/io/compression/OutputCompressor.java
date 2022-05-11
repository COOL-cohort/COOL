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

import com.nus.cool.core.io.Output;
import com.nus.cool.core.schema.Codec;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Utility class for writing a compressed integer/string vector into disk.
 */
public class OutputCompressor implements Output {

  // int or string,
  private DataType dataType;
  // Integers vector to be compressed
  private int[] vec;
  // String vector to be compressed
  private byte[] strVec;
  // the start offset in the data
  private int off;
  // the number of bytes read
  private int len;
  // statistic information of the vec / strVec
  private Histogram hist;

  /**
   * @param h   histogram of compress data
   * @param vec compress data
   * @param off the start offset in the data
   * @param len the number of bytes read
   */
  public void reset(Histogram h, int[] vec, int off, int len) {
    this.hist = h;
    this.vec = vec;
    this.off = off;
    this.len = len;
    this.dataType = DataType.INTEGER;
  }

  /**
   * @param h   histogram of compress data
   * @param vec compress data
   * @param off the start offset in the data
   * @param len the number of bytes read
   */
  public void reset(Histogram h, byte[] vec, int off, int len) {
    this.hist = h;
    this.strVec = vec;
    this.off = off;
    this.len = len;
    this.dataType = DataType.STRING;
  }

  @Override
  public int writeTo(DataOutput out) throws IOException {
    int bytesWritten = 0;
    // 1. select a compressor type
    Codec codec = CompressorAdviser.advise(this.hist);
    // 2. create compressor instance according to the type
    Compressor compressor = CompressorFactory.newCompressor(codec, this.hist);
    int maxLen = compressor.maxCompressedLength();
    // 3. compress it and record output to compressed array
    byte[] compressed = new byte[maxLen];
    int compressLen = this.dataType == DataType.INTEGER ?
        compressor.compress(this.vec, this.off, this.len, compressed, 0, maxLen)
        : compressor.compress(this.strVec, this.off, this.len, compressed, 0, maxLen);

    // Write compressor type
    out.writeByte(codec.ordinal());
    bytesWritten++;
    // Write compressed data
    out.write(compressed, 0, compressLen);
    bytesWritten += compressLen;
    return bytesWritten;
  }

  private enum DataType {INTEGER, STRING}
}
