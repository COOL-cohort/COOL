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
import java.util.BitSet;

/**
 * Input vector of a bit vector structure.
 */
public class BitVectorInputVector implements InputVector<Integer> {

  private long[] words;

  private int[] lookupTable;

  private int[] globalIDs;

  private int numOfIDs;

  @Override
  public int size() {
    return this.numOfIDs;
  }

  @Override
  public Integer find(Integer key) {
    int i = wordIndex(key);
    int j = remainder(key);
    long bits = this.words[i] << (63 - j);
    return (bits < 0) ? (Long.bitCount(bits) + this.lookupTable[i] - 1) : -1;
  }

  @Override
  public Integer get(int index) {
    if (this.globalIDs.length == 0) {
      return 0;
    }
    return this.globalIDs[index];
  }

  @Override
  public boolean hasNext() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Integer next() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void skipTo(int pos) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readFrom(ByteBuffer buffer) {
    int len = buffer.get() & 0xFF;
    this.words = new long[len];
    this.lookupTable = new int[len];

    this.lookupTable[0] = 0;
    this.words[0] = buffer.getLong();

    for (int i = 1; i < len; i++) {
      this.words[i] = buffer.getLong();
      this.lookupTable[i] = Long.bitCount(this.words[i - 1]) + this.lookupTable[i - 1];
    }
    this.numOfIDs = this.lookupTable[len - 1] + Long.bitCount(this.words[len - 1]);
    this.fillInGlobalIDs();
  }

  private int wordIndex(int i) {
    return i >>> 6;
  }

  private int remainder(int i) {
    return i & (64 - 1);
  }

  private void fillInGlobalIDs() {
    BitSet bs = BitSet.valueOf(this.words);
    this.globalIDs = new int[this.numOfIDs];
    for (int i = bs.nextSetBit(0), j = 0; i >= 0; i = bs.nextSetBit(i + 1)) {
      this.globalIDs[j++] = i;
    }
  }
}
