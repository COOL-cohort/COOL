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

import com.google.common.primitives.Ints;
import com.nus.cool.core.util.IntBuffers;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

/**
 *
 * @author zhongle
 * @version 0.1
 * @since 0.1
 */
public class ZInt32Store implements ZIntStore, InputVector {

  private int count;

  private IntBuffer buffer;

  public ZInt32Store(int count) {
    this.count = count;
  }

  public static ZIntStore load(ByteBuffer buffer, int n) {
    ZIntStore store = new ZInt32Store(n);
    store.readFrom(buffer);
    return store;
  }

  @Override
  public int size() {
    return this.count;
  }

  @Override
  public int find(int key) {
    return IntBuffers.binarySearch(this.buffer, 0, this.buffer.limit(), key);
  }

  @Override
  public int get(int index) {
    return this.buffer.get(index);
  }

  @Override
  public boolean hasNext() {
    return this.buffer.hasRemaining();
  }

  @Override
  public int next() {
    return this.buffer.get();
  }

  @Override
  public void skipTo(int pos) {
    this.buffer.position(pos);
  }

  @Override
  public void readFrom(ByteBuffer buffer) {
    int limit = buffer.limit();
    int newLimit = buffer.position() + this.count * Ints.BYTES;
    buffer.limit(newLimit);
    this.buffer = buffer.asIntBuffer();
    buffer.position(newLimit);
    buffer.limit(limit);
  }
}
