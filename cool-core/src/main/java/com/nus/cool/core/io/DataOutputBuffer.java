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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Output buffer abstraction.
 */
public class DataOutputBuffer extends DataOutputStream {

  private Buffer buffer;

  public DataOutputBuffer() {
    this(new Buffer());
  }

  public DataOutputBuffer(Buffer buffer) {
    super(buffer);
    this.buffer = buffer;
  }

  public byte[] getData() {
    return this.buffer.getData();
  }

  public int getLength() {
    return this.buffer.getLength();
  }

  /**
   * Write input to the buffer.
   *
   * @param in     the input stream
   * @param length an int specifying the number of bytes to read
   * @throws IOException If an I/O error occurs
   */
  public void write(DataInput in, int length) throws IOException {
    this.buffer.write(in, length);
  }

  /**
   * Write to file stream.
   *
   * @param out out stream
   * @throws IOException if an I/O error occurs.
   */
  public void writeTo(OutputStream out) throws IOException {
    this.buffer.writeTo(out);
  }

  /**
   * Overwrite an integer into the internal buffer. Note that this call can only be used to
   *  overwrite existing data in the buffer, i.e., buffer#count cannot be increased, and
   *  DataOutputStream#written cannot be increased.
   *
   * @param v      overwrite integer
   * @param offset overwrite offset
   * @throws IOException if an I/O error occurs.
   */
  public void writeInt(int v, int offset) throws IOException {
    checkState(offset + 4 <= this.buffer.getLength());
    byte[] b = new byte[4];
    b[0] = (byte) ((v >>> 24) & 0xFF);
    b[1] = (byte) ((v >>> 16) & 0xFF);
    b[2] = (byte) ((v >>> 8) & 0xFF);
    b[3] = (byte) (v & 0xFF);
    int oldCount = this.buffer.setCount(offset);
    this.buffer.write(b);
    this.buffer.setCount(oldCount);
  }

  private static class Buffer extends FastOutputStream {

    public Buffer() {
      super();
    }

    // public Buffer(int size) {
    //   super(size);
    // }

    public byte[] getData() {
      return this.buffer;
    }

    public int getLength() {
      return this.count;
    }

    /**
     * Write len bytes from an input stream.
     *
     * @param in  the input stream
     * @param len an int specifying the number of bytes to read
     * @throws IOException If an I/O error occurs
     */
    public void write(DataInput in, int len) throws IOException {
      int newCount = this.count + len;
      if (newCount > this.buffer.length) {
        byte[] newBuffer = new byte[Math.max(this.buffer.length << 1, newCount)];
        System.arraycopy(this.buffer, 0, newBuffer, 0, this.count);
        this.buffer = newBuffer;
      }
      in.readFully(this.buffer, this.count, len);
      this.count = newCount;
    }

    /**
     * Set the count for the current buffer.
     *
     * @param newCount the new count to set
     * @return the original count
     */
    private int setCount(int newCount) {
      checkArgument(newCount >= 0 && newCount <= this.buffer.length);
      int oldCount = this.count;
      this.count = newCount;
      return oldCount;
    }
  }
}
