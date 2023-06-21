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

package com.nus.cool.core.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.ByteBuffer;

/**
 * ByteBuffers represents a byte buffer. With ByteBuffers and a key of byte,
 * ByteBuffers could find the key in buffer and return the result with binary
 * research
 */
public class ByteBuffers {

  /**
   * Search index of key by binary search.
   *
   * @param buffer    data of byte
   * @param fromIndex from index in buffer
   * @param toIndex   to index in buffer
   * @param key       search param
   * @return index of key in buffer
   */
  public static int binarySearchUnsigned(ByteBuffer buffer, int fromIndex, int toIndex, byte key) {
    checkNotNull(buffer);
    checkArgument(fromIndex < buffer.limit() && toIndex <= buffer.limit());

    int ikey = key & 0xFF;
    toIndex--;
    while (fromIndex <= toIndex) {
      int mid = (fromIndex + toIndex) >> 1;
      int e = buffer.get(mid) & 0xFF;
      if (ikey > e) {
        fromIndex = mid + 1;
      } else if (ikey < e) {
        toIndex = mid - 1;
      } else {
        return mid;
      }
    }
    return ~fromIndex;
  }

  /**
   *  this is traverseSearch.
   *
   * @param buffer buffer.
   * @param fromIndex fromIndex.
   * @param toIndex toIndex.
   * @param key key.
   * @return value
   */
  public static int traverseSearch(ByteBuffer buffer, int fromIndex, int toIndex, byte key) {
    checkNotNull(buffer);
    checkArgument(fromIndex < buffer.limit() && toIndex <= buffer.limit());
    // int ikey =
    int ikey = key & 0xFF;
    for (int i = fromIndex; i < toIndex; i++) {
      int e = buffer.get(i) & 0xFF;
      if (e == ikey) {
        return i;
      }
    }
    return -1;
  }
}
