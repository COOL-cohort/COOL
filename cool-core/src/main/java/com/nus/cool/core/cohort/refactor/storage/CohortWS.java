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
package com.nus.cool.core.cohort.refactor.storage;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.primitives.Ints;
import com.nus.cool.core.io.Output;
import com.nus.cool.core.io.compression.Compressor;
import com.nus.cool.core.io.compression.Histogram;
import com.nus.cool.core.io.compression.ZIntBitCompressor;
import com.nus.cool.core.util.IntegerUtil;


/**
 * CohortWS writes the list of users found by a cohort selection query to a file
 * COOL store the file under the version of cube queried.
 */
public class CohortWS implements Output {

  /**
   * a list userid for each cublet (a user can have different id across cublet)
   */
  private List<List<Integer>> usersByCublet;
  
  public CohortWS(int numCublets) {
    this.usersByCublet = new ArrayList<>(
      (numCublets > 0) ? numCublets : 0);
  }

  /**
   * add cohort user results of the next cublet.
   * (assuming results are added in sequence)
   * @param users : users' id in this cublet
   */
  public void addCubletResults(List<Integer> users) {
    usersByCublet.add(users);
  }
  
  /**
   * write the users' id of one cublet
   * 
   * @param out
   * @return
   */
  private int writeTo(List<Integer> users, DataOutput out) throws IOException {
    Compressor compressor = new ZIntBitCompressor(
      Histogram.builder()
               .max(Collections.max(users))
               .numOfValues(users.size())
               .uniqueValues(users.size())
               .build() 
    );
    byte[] compressed = new byte[compressor.maxCompressedLength()];
    int nbytes = compressor.compress(users.stream().mapToInt(i->i).toArray(),
      0, users.size(), compressed, 0, compressed.length);

    out.write(compressed);
    return nbytes;
  }

  @Override
  public int writeTo(DataOutput out) throws IOException {
    int[] offsets = new int[usersByCublet.size()];
    // initial offset is 0
    offsets[0] = 0;
    for (int i = 0; i < usersByCublet.size()-1; i++) {
      offsets[i+1] = offsets[0] + writeTo(usersByCublet.get(i), out);
    }
    int headerOffset = offsets[offsets.length-1]
      + writeTo(usersByCublet.get(usersByCublet.size()), out);
    for (int offset : offsets) {
      out.writeInt(IntegerUtil.toNativeByteOrder(offset));
    }
    out.writeInt(IntegerUtil.toNativeByteOrder(headerOffset));
    return headerOffset + Ints.BYTES * (offsets.length + 1);
  }
}
