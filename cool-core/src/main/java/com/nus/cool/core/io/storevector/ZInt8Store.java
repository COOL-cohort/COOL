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
package com.nus.cool.core.io.storevector;

import com.nus.cool.core.util.ByteBuffers;
import java.nio.ByteBuffer;

/**
 * Decompress data which stores integers in one byte
 * <p>
 * The data layout is as follows
 * ------------------------------------
 * | count | ZInt compressed integers |
 * ------------------------------------
 */
public class ZInt8Store implements InputVector, ZIntStore {

  /**
   * number of values
   */
  private int count;

  /**
   * compressed data
   */
  private ByteBuffer buffer;

  public ZInt8Store(int count) {
    this.count = count;
  }

  public static ZIntStore load(ByteBuffer buffer, int n) {
    ZIntStore store = new ZInt8Store(n);
    store.readFrom(buffer);
    return store;
  }

  @Override
  public int size() {
    return this.count;
  }

  @Override
  public int find(int key) {
    if (key > Byte.MAX_VALUE || key < 0) {
        return -1;
    }
    return ByteBuffers.binarySearchUnsigned(this.buffer, 0, this.buffer.limit(), (byte) key);
  }

  @Override
  public int get(int index) {
    return (this.buffer.get(index) & 0xFF);
  }

  @Override
  public boolean hasNext() {
    return this.buffer.hasRemaining();
  }

  @Override
  public int next() {
    return (this.buffer.get() & 0xFF);
  }

  @Override
  public void skipTo(int pos) {
    this.buffer.position(pos);
  }

  @Override
  public void readFrom(ByteBuffer buffer) {
    int limit = buffer.limit();
    int newLimit = buffer.position() + this.count;
    buffer.limit(newLimit);
    this.buffer = buffer.slice();
    buffer.position(newLimit);
    buffer.limit(limit);
  }
}
