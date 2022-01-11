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

import static com.google.common.base.Preconditions.checkArgument;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 *
 * @author zhongle
 * @version 0.1
 * @since 0.1
 */
public class LZ4InputVector implements InputVector {

  private int zLen;

  private int rawLen;

  private ByteBuffer buffer;

  private int[] offsets;

  private byte[] data;

  private LZ4FastDecompressor decompressor = LZ4Factory.fastestInstance().fastDecompressor();

  @Override
  public int size() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int find(int key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int get(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasNext() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int next() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void skipTo(int pos) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readFrom(ByteBuffer buffer) {
    this.zLen = buffer.getInt();
    this.rawLen = buffer.getInt();
    int oldLimit = buffer.limit();
    int newLimit = buffer.position() + this.zLen;
    buffer.limit(newLimit);
    this.buffer = buffer.slice().order(buffer.order());
    buffer.position(newLimit);
    buffer.limit(oldLimit);
  }

  public String getString(int index, Charset charset) {
    if (this.buffer.hasRemaining()) {
      byte[] compressed = new byte[this.zLen];
      byte[] raw = new byte[this.rawLen];
      this.buffer.get(compressed);
      this.decompressor.decompress(compressed, raw, this.rawLen);
      ByteBuffer buffer = ByteBuffer.wrap(raw);
      int values = buffer.getInt();
      this.offsets = new int[values];
        for (int i = 0; i < values; i++) {
            this.offsets[i] = buffer.getInt();
        }
      this.data = new byte[rawLen - 4 - values * 4];
      buffer.get(this.data);
    }
    checkArgument(index < this.offsets.length && index >= 0);
    int last = this.offsets.length - 1;
    int off = this.offsets[index];
    int end = index == last ? this.data.length : this.offsets[index + 1];
    int len = end - off;
    byte[] tmp = new byte[len];
    System.arraycopy(this.data, off, tmp, 0, len);
    return new String(tmp, charset);
  }
}
