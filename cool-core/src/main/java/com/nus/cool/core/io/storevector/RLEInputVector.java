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

/**
 * Input vector of a run-length-encoded structure.
 */
public class RLEInputVector implements InputVector<Integer> {

  // total number of blocks
  private int blks;

  private ByteBuffer buffer;

  private int curBlk;

  // begin offset
  private int boff;

  // end offset
  private int bend;

  // current value
  private int bval;

  @Override
  public int size() {
    return 0;
  }

  @Override
  public Integer find(Integer key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Integer get(int index) {
    int offset = this.boff;
    this.skipTo(index); // skip to one block, and read the value
    int v = next();
    this.boff = offset;
    return v;
  }

  @Override
  public boolean hasNext() {
    return this.curBlk < this.blks || this.boff < this.bend;
  }

  @Override
  public Integer next() {
    if (this.boff < this.bend) {
      this.boff++;
      return bval;
    }
    readNextBlock();
    this.boff++;
    return this.bval;
  }

  @Override
  public void skipTo(int pos) {
    if (pos < this.boff) {
      this.buffer.rewind();
      this.curBlk = 0;
      readNextBlock();
    }
    while (pos >= this.bend && this.curBlk < this.blks) {
      readNextBlock();
    }
    if (pos >= this.bend) {
      throw new IllegalArgumentException("Too large pos param");
    }
    this.boff = pos;
  }

  @Override
  public void readFrom(ByteBuffer buffer) {
    final int zLen = buffer.getInt();
    this.blks = buffer.getInt();
    final int oldLimit = buffer.limit();
    int newLimit = buffer.position() + zLen;
    buffer.limit(newLimit);
    this.buffer = buffer.slice().order(buffer.order());
    buffer.position(newLimit);
    buffer.limit(oldLimit);
  }

  /**
   * Move to the next block.
   */
  public void nextBlock(Block blk) {
    if (this.boff < this.bend) {
      blk.value = this.bval;
      blk.off = this.boff;
      blk.len = this.bend - this.boff;
      this.boff = this.bend;
      return;
    }
    readNextBlock();
    blk.value = this.bval;
    blk.off = this.boff;
    blk.len = this.bend - this.boff;
    this.boff = this.bend;
  }

  /**
   * read block, and update current value.
   * read 8 bites first,
   * first two bites => size of value
   * medium two bites => size of boff
   * last two bites => size of bend
   */
  private void readNextBlock() {
    int b = this.buffer.get();
    this.bval = read((b >> 4) & 3);
    this.boff = read((b >> 2) & 3);
    this.bend = this.boff + read(b & 3);
    this.curBlk++;
  }

  // todo(naili) why do we need & 0xff, already read same bytes.
  private int read(int width) {
    switch (width) {
      case 1:
        // Reads the byte and get the lower 8 bites
        return this.buffer.get() & 0xff;
      case 2:
        // read the next two bytes and get the lower 16 bites
        return this.buffer.getShort() & 0xffff;
      case 3:
      case 0:
        // get the lower 32 bites
        return this.buffer.getInt();
      default:
        throw new IllegalArgumentException("Incorrect number of bytes");
    }
  }

  /**
   * Data block.
   */
  public static class Block {
    public int value;
    public int off;
    public int len;
  }
}
