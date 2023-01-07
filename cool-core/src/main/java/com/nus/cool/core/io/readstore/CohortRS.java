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

package com.nus.cool.core.io.readstore;

import com.nus.cool.core.io.Input;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.ZIntBitInputVector;
import java.nio.ByteBuffer;

/**
 * Read store of the cohort result.
 */
public class CohortRS implements Input {

  private InputVector<Integer> userList;

  private final ByteBuffer buffer;

  private final int startPos;

  private String query;

  private CohortRS(ByteBuffer buffer) {
    this.buffer = buffer.asReadOnlyBuffer().order(buffer.order());
    startPos = buffer.position();
  }

  /**
   * Load the cohort result from a buffer.
   */
  public static CohortRS load(ByteBuffer buffer) {
    CohortRS st = new CohortRS(buffer);
    // st.readFrom(buffer);
    return st;
  }

  /**
   * Return the users in the cohort as an input vector.
   */
  public InputVector<Integer> getUsers() {
    // need to get a new input vector
    readFrom(buffer);
    return userList;
  }

  /**
   * Return the query that is used to generate this cohort.
   */
  public String getQuery() {
    if (query == null) {
      readFrom(buffer);
    }
    return query;
  }

  @Override
  public void readFrom(ByteBuffer buffer) {
    buffer.position(startPos);
    userList = ZIntBitInputVector.load(buffer);
    if (query == null) {
      byte[] dest = new byte[buffer.remaining()];
      buffer.get(dest);
      this.query = new String(dest);
    }
  }
}
