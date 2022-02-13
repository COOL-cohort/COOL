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

public interface Compressor {

  /**
   * Estimate the maximum size of compressed data in byte
   *
   * @return number of bytes
   */
  int maxCompressedLength();

  /**
   * Compress a byte array
   *
   * @param src        the compressed data
   * @param srcOff     the start offset in sec
   * @param srcLen     the number of bytes to compress
   * @param dest       the destination buffer
   * @param destOff    the start offset in dest
   * @param maxDestLen the maximum number of bytes to write in dest
   * @return the compressed size
   */
  int compress(byte[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen);

  /**
   * Compress an integer array
   *
   * @param src        the compressed data
   * @param srcOff     the start offset in sec
   * @param srcLen     the number of bytes to compress
   * @param dest       the destination buffer
   * @param destOff    the start offset in dest
   * @param maxDestLen the maximum number of bytes to write in dest
   * @return the compressed size
   */
  int compress(int[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen);

}
