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

import com.google.common.primitives.Shorts;
import com.nus.cool.core.util.ShortBuffers;
import java.nio.ByteBuffer;
import java.nio.ShortBuffer;

/**
 * Decompress data which stores integers in two bytes.
 * <p>
 * The data layout is as follows
 * ------------------------------------
 * | count | ZInt compressed integers |
 * ------------------------------------
 */
public class ZInt16Store implements ZIntStore {

  private int count;

  private boolean sorted;

  private ShortBuffer buffer;

  @Override
  public int size() {
    return this.count;
  }

  @Override
  public Integer find(Integer key) {
    if (key > Short.MAX_VALUE || key < 0) {
      return -1;
    }
    if (this.sorted) {
      return ShortBuffers.binarySearchUnsigned(this.buffer, 0,
        this.buffer.limit(), key.shortValue());
    } else {
      return ShortBuffers.traverseSearch(this.buffer, 0, this.buffer.limit(), key.shortValue());
    }
  }

  @Override
  public Integer get(int index) {
    return (this.buffer.get(index) & 0xFFFF);
  }

  @Override
  public boolean hasNext() {
    return this.buffer.hasRemaining();
  }

  @Override
  public Integer next() {
    return (this.buffer.get() & 0xFFFF);
  }

  @Override
  public void skipTo(int pos) {
    this.buffer.position(pos);
  }

  @Override
  public void readFrom(ByteBuffer buffer) {
    this.count = buffer.getInt();
    int flag = buffer.get(); // get byte into int
    this.sorted = flag == 1;
    int limit = buffer.limit();
    int newLimit = buffer.position() + this.count * Shorts.BYTES;
    buffer.limit(newLimit);
    this.buffer = buffer.asShortBuffer();
    buffer.position(newLimit);
    buffer.limit(limit);
  }
}
