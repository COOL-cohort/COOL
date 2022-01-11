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

import com.nus.cool.core.schema.Codec;
import java.nio.ByteBuffer;

/**
 *
 * @author zhongle
 * @version 0.1
 * @since 0.1
 */
public class FoRInputVector implements InputVector {

  private int min;

  private int max;

  private InputVector vecIn;

  @Override
  public int size() {
    return this.vecIn.size();
  }

  @Override
  public int find(int key) {
      if (key < this.min || key > this.max) {
          return -1;
      }
    return this.vecIn.find(key - this.min);
  }

  @Override
  public int get(int index) {
    return this.min + this.vecIn.get(index);
  }

  @Override
  public boolean hasNext() {
    return this.vecIn.hasNext();
  }

  @Override
  public int next() {
    return this.min + this.vecIn.next();
  }

  @Override
  public void skipTo(int pos) {
    this.vecIn.skipTo(pos);
  }

  @Override
  public void readFrom(ByteBuffer buffer) {
    this.min = buffer.getInt();
    this.max = buffer.getInt();
    Codec codec = Codec.fromInteger(buffer.get());
    switch (codec) {
      case INT8:
        this.vecIn = (InputVector) ZInt8Store.load(buffer, buffer.getInt());
        break;
      case INT16:
        this.vecIn = (InputVector) ZInt16Store.load(buffer, buffer.getInt());
        break;
      case INT32:
        this.vecIn = (InputVector) ZInt32Store.load(buffer, buffer.getInt());
        break;
      default:
        throw new IllegalArgumentException("Unsupported codec: " + codec);
    }
  }
}
