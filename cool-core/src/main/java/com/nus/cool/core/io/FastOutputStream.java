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

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

/**
 * Fast ByteArrayOutputStream implementation without any synchronization.
 */
public class FastOutputStream extends OutputStream {

  /**
   * The buffer where data is stored.
   */
  protected byte[] buffer;

  /**
   * The number of valid bytes in the buffer.
   */
  protected int count;

  /**
   * Creates a new bute array output stream. The buffer capacity in initially 32 bytes,
   *  though its size increases if necessary.
   */
  public FastOutputStream() {
    this(32);
  }

  /**
   * Creates a new byte array output stream, with a buffer capacity of the
   * specified size, in
   * bytes.
   *
   * @param size the initial size.
   * @throws IllegalArgumentException if size is negative.
   */
  public FastOutputStream(int size) {
    if (size < 0) {
      throw new IllegalArgumentException("Negative initial size: " + size);
    }
    this.buffer = new byte[size];
  }

  /**
   * Writes the specified byte to this byte array output stream.
   *
   * @param b the byte to be written
   */
  public void write(int b) {
    ensureCapacity(this.count + 1);
    this.buffer[this.count] = (byte) b;
    this.count += 1;
  }

  /**
   * Writes <code>len</code> bytes from the specified byte array starting at
   * offset <code>off</code>
   * to this byte array output stream.
   *
   * @param b   the data
   * @param off the start offset in the data
   * @param len the number of bytes to write
   */
  public void write(byte[] b, int off, int len) {
    if (off < 0 || off > b.length || len < 0 || (off + len) - b.length > 0) {
      throw new IndexOutOfBoundsException();
    }
    ensureCapacity(this.count + len);
    System.arraycopy(b, off, this.buffer, this.count, len);
    this.count += len;
  }

  /**
   * Writes the complete contents of this byte array output strean ti the
   * specified output stream
   * argument, as if by calling the output stream's write method using
   * <code>out.write(buffer, 0,
   * count)</code>.
   *
   * @param out the output stream to which to write the data.
   * @throws IOException if an I/O error occurs.
   */
  public void writeTo(OutputStream out) throws IOException {
    out.write(this.buffer, 0, this.count);
  }

  /**
   * Resets the <code>count</code> field of this byte array output stream to zero,
   * so that all
   * currently accumulated output in the output stream is discarded. The output
   * stream can be used
   * again, reusing the already allocated buffer space.
   *
   * @see java.io.ByteArrayInputStream
   */
  public void reset() {
    this.count = 0;
  }

  /**
   * Creates a newly allocated byte array. Its size is the current size of this
   * output stream and
   * the valid contents of the buffer have been copied into it.
   *
   * @return the current contents of this output stream, as a byte array.
   * @see java.io.ByteArrayOutputStream#size()
   */
  public byte[] toByteArray() {
    return Arrays.copyOf(this.buffer, this.count);
  }

  /**
   * Returns the current size of the buffer.
   *
   * @return the value of the <code>count</code> field, which is the number of
   *         valid bytes in this
   *         output stream.
   * @see java.io.ByteArrayOutputStream
   */
  public int size() {
    return this.count;
  }

  /**
   * Converts the buffer's contents into a string decoding bytes using the
   * platform's default
   * character set. The length of the new <tt>String</tt> is a function of the
   * character set, and
   * hence may not be equal to the size of the buffer.
   *
   * <p>
   * This method always replaces malformed-input and unmappable-character
   * sequences with the default
   * replacement string for the platform's default character set. The {@linkplain
   * java.nio.charset.CharsetDecoder} class should be used when more control over
   * the decoding
   * process is required.
   *
   * @return String decoded from the buffer's contents.
   * @since JDK1.1
   */
  public String toString() {
    return new String(this.buffer, 0, this.count);
  }

  /**
   * Converts the buffer's contents into a string by decoding the bytes using the
   * specified {@link
   * java.nio.charset.Charset charsetName}. The length of the new
   * <tt>String</tt> is a function of the charset, and hence may not be equal to
   * the length of the byte array.
   *
   * <p>
   * This method always replaces malformed-input and unmappable-character
   * sequences with this
   * charset's default replacement string. The
   * {@link java.nio.charset.CharsetDecoder} class should
   * be used when more control over the decoding process is required.
   *
   * @param charsetName the name of a supported
   * @return String decoded from the buffer's contents.
   * @throws UnsupportedEncodingException If the named charset is not supported
   * @since JDK1.1
   */
  public String toString(String charsetName) throws UnsupportedEncodingException {
    return new String(this.buffer, 0, this.count, charsetName);
  }

  /**
   * Closing a <tt>ByteArrayOutputStream</tt> has no effect. The methods in this
   * class can be called
   * after the stream has been closed without generating an
   * <tt>IOException</tt>.
   * <p>
   */
  public void close() throws IOException {
  }

  /**
   * Increases the capacity if necessary to ensure that it can hold at least the
   * number of elements
   * specified by minimum capacity argument.
   *
   * @param minCapacity the desired minimum capacity
   * @throws OutOfMemoryError if {@code minCapacity < 0}. This is interpreted as a
   *                          request for the
   *                          unsatisfiable large capacity
   *                          {@code (long) Integer.MAX_VALUE +
   *                          (minCapacity - Integers.MAX_VALUE)}
   */
  private void ensureCapacity(int minCapacity) {
    if (minCapacity - this.buffer.length > 0) {
      grow(minCapacity);
    }
  }

  /**
   * Increases the capacity to ensure that it can hold at least the number of
   * elements specified by
   * the minimum capacity argument.
   *
   * @param minCapacity the desired minimum capacity
   */
  private void grow(int minCapacity) {
    int oldCapacity = this.buffer.length;
    int newCapacity = oldCapacity << 1;
    if (newCapacity - minCapacity < 0) {
      newCapacity = minCapacity;
    }
    if (newCapacity < 0) {
      // overflow. TODO: Check whether the following condition is always false
      if (minCapacity < 0) {
        // lgtm [java/constant-comparison]
        throw new OutOfMemoryError();
      }
      newCapacity = Integer.MAX_VALUE;
    }
    this.buffer = Arrays.copyOf(this.buffer, newCapacity);
  }
}
