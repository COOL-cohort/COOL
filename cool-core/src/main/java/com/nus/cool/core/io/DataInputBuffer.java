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

package com.nus.cool.core.io;

import java.io.DataInputStream;

/**
 * Input buffer abstraction.
 */
public class DataInputBuffer extends DataInputStream {

  private Buffer buffer;

  public DataInputBuffer() {
    this(new Buffer());
  }

  public DataInputBuffer(Buffer buffer) {
    super(buffer);
    this.buffer = buffer;
  }

  /**
   * Resets te data that the buffer reads.
   *
   * @param input  the input stream
   * @param length number of bytes to read
   */
  public void reset(byte[] input, int length) {
    this.buffer.reset(input, 0, length);
  }

  /**
   * Resets te data that the buffer reads.
   *
   * @param input the input stream
   */
  public void reset(DataOutputBuffer input) {
    reset(input.getData(), input.size());
  }

  public byte[] getData() {
    return this.buffer.getData();
  }

  /** Returns the current position in the input. */
  public int getPosition() {
    return this.buffer.getPosition();
  }

  /**
   * Returns the index one greater than the last valid character in the input
   * stream buffer.
   */
  public int getLength() {
    return this.buffer.getLength();
  }

  private static class Buffer extends FastInputStream {

    public Buffer() {
      super(new byte[] {});
    }

    public void reset(byte[] input, int start, int length) {
      this.buffer = input;
      this.count = start + length;
      this.mark = start;
      this.pos = start;
    }

    public byte[] getData() {
      return this.buffer;
    }

    public int getPosition() {
      return this.pos;
    }

    public int getLength() {
      return this.count;
    }
  }
}
