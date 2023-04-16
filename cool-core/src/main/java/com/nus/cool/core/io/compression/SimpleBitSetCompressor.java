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

package com.nus.cool.core.io.compression;

import com.google.common.primitives.Ints;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Compress BitSet using rle schema, prefer sorted BitSet.
 * 
 * <p>
 * Data layout
 * ------------------------------------
 * | sign | blk1 | blk2 | ... | blk n |
 * ------------------------------------
 * where
 * sign = bit, the first bit of BitSet
 * blk = number of bits for the same bit
 */
public class SimpleBitSetCompressor {

  /**
   * Compress biset.
   */
  public static int compress(BitSet bs, DataOutput out) throws IOException {
    int pos1 = 0;
    int pos2;
    int bytesWritten = 0;
    List<Integer> blks = new ArrayList<>();
    boolean sign = bs.get(pos1);
    while (true) {
      if (sign) {
        pos2 = bs.nextClearBit(pos1);
      } else {
        pos2 = bs.nextSetBit(pos1);
      }
      if (pos2 < 0 & !sign) {
        break;
      }
      blks.add(pos2 - pos1);
      pos1 = pos2;
      sign = !sign;
    }
    out.write(bs.get(0) ? 1 : 0);
    bytesWritten++;
    out.writeInt(blks.size());
    bytesWritten += Ints.BYTES;
    for (int blk : blks) {
      out.writeInt(blk);
      bytesWritten += Ints.BYTES;
    }
    return bytesWritten;
  }

  /**
   * Decompress a compressed biset buffer.
   */
  public static BitSet read(ByteBuffer buff) {
    boolean sign = buff.get() != 0;
    int blks = buff.getInt();
    BitSet bs = new BitSet();
    int pos = 0;
    int len;
    for (int i = 0; i < blks; i++) {
      len = buff.getInt();
      if (sign) {
        bs.set(pos, pos + len);
      }
      pos = pos + len;
      sign = !sign;
    }
    return bs;
  }

}
