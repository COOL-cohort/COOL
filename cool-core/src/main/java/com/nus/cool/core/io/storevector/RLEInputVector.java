/*
 * Copyright 2020 Cool Squad Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nus.cool.core.io.storevector;

import java.nio.ByteBuffer;

/**
 *
 * @author zhongle
 * @version 0.1
 * @since 0.1
 */
public class RLEInputVector implements InputVector {

  private int blks;

  private ByteBuffer buffer;

  private int curBlk;

  private int boff;

  private int bend;

  private int bval;

  @Override
  public int size() {
    return 0;
  }

  @Override
  public int find(int key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int get(int index) {
    int offset = this.boff;
    this.skipTo(index);
    int v = next();
    this.boff = offset;
    return v;
  }

  @Override
  public boolean hasNext() {
    return this.curBlk < this.blks || this.boff < this.bend;
  }

  @Override
  public int next() {
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
    int zLen = buffer.getInt();
    this.blks = buffer.getInt();
    int oldLimit = buffer.limit();
    int newLimit = buffer.position() + zLen;
    buffer.limit(newLimit);
    this.buffer = buffer.slice().order(buffer.order());
    buffer.position(newLimit);
    buffer.limit(oldLimit);
  }

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

  private void readNextBlock() {
    int b = this.buffer.get();
    this.bval = read((b >> 4) & 3);
    this.boff = read((b >> 2) & 3);
    this.bend = this.boff + read(b & 3);
    this.curBlk++;
  }

  private int read(int width) {
    switch (width) {
      case 1:
        return this.buffer.get() & 0xff;
      case 2:
        return this.buffer.getShort() & 0xffff;
      case 3:
      case 0:
        return this.buffer.getInt();
      default:
        throw new IllegalArgumentException("Incorrect number of bytes");
    }
  }

  public static class Block {

    public int value;

    public int off;

    public int len;

  }
}
