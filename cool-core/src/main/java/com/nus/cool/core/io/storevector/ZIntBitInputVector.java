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

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

/**
 * Input vector of ZIntBit compressed structure.
 */
public class ZIntBitInputVector implements ZIntStore {

  private final LongBuffer bitPack;

  private final int capacity;
  private final int bitWidth;
  private final int noValPerPack;
  private final long mask;

  private int pos;
  private long curPack;
  private int packOffset;

  private ZIntBitInputVector(LongBuffer buffer, int capacity, int bitWidth) {
    this.capacity = capacity;
    this.bitWidth = bitWidth;
    this.noValPerPack = 64 / bitWidth;
    this.mask = (bitWidth == 64) ? -1 : (1L << bitWidth) - 1;
    this.bitPack = buffer;
  }

  /**
   * Create input vector on a buffer that is ZIntBit encoded.
   */
  public static ZIntBitInputVector load(ByteBuffer buffer) {
    int capacity = buffer.getInt();
    int width = buffer.getInt();
    int size = getNumOfBytes(capacity, width);
    int oldLimit = buffer.limit();
    buffer.limit(buffer.position() + size - 8);
    LongBuffer tmpBuffer = buffer.asLongBuffer();
    buffer.position(buffer.position() + size - 8);
    buffer.limit(oldLimit);
    return new ZIntBitInputVector(tmpBuffer, capacity, width);
  }

  private static int getNumOfBytes(int num, int width) {
    int i = 64 / width;
    int size = (num - 1) / i + 2;
    return size << 3;
  }

  @Override
  public int size() {
    return this.capacity;
  }

  @Override
  public Integer find(Integer key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Integer get(int index) {
    if (index >= this.capacity) {
      throw new IndexOutOfBoundsException();
    }
    int offset = 64 - (1 + index % this.noValPerPack) * this.bitWidth;
    long pack = getPack(index);
    long val = ((pack >>> offset) & this.mask);
    return (int) val;
  }

  @Override
  public boolean hasNext() {
    return this.pos < this.capacity;
  }

  @Override
  public Integer next() {
    return (int) nextLong();
  }

  @Override
  public void skipTo(int pos) {
    if (pos >= this.capacity) {
      throw new IndexOutOfBoundsException();
    }
    this.pos = pos;
    this.packOffset = 64 - (pos % this.noValPerPack) * this.bitWidth;
    this.bitPack.position(pos / this.noValPerPack);
    this.curPack = this.bitPack.get();
  }

  @Override
  public void readFrom(ByteBuffer buffer) {
    throw new UnsupportedOperationException();
  }

  private long getPack(int pos) {
    int idx = pos / this.noValPerPack;
    return this.bitPack.get(idx);
  }

  private long nextLong() {
    if (this.packOffset < this.bitWidth) {
      this.curPack = this.bitPack.get();
      this.packOffset = 64;
    }
    this.pos++;
    this.packOffset -= this.bitWidth;
    return ((this.curPack >>> this.packOffset) & this.mask);
  }
}
