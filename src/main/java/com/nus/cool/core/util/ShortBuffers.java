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
package com.nus.cool.core.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.ShortBuffer;

/**
 *
 * @author zhongle
 * @version 0.1
 * @since 0.1
 */
public class ShortBuffers {

  /**
   * Search index of key by binary search
   *
   * @param buffer    data
   * @param fromIndex from index in buffer
   * @param toIndex   to index in buffer
   * @param key       search param
   * @return index of key in buffer
   */
  public static int binarySearchUnsigned(ShortBuffer buffer, int fromIndex, int toIndex,
      short key) {
    checkNotNull(buffer);
    checkArgument(fromIndex < buffer.limit() && toIndex <= buffer.limit());

    int ikey = key & 0xFFFF;
    toIndex--;
    while (fromIndex <= toIndex) {
      int mid = (fromIndex + toIndex) >> 1;
      int e = buffer.get(mid) & 0xFFFF;
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
}
