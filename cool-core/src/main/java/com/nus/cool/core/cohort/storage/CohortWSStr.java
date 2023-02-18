
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

package com.nus.cool.core.cohort.storage;

import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.ValueWrapper;
import com.nus.cool.core.io.Output;
import com.nus.cool.core.io.compression.Histogram;
import com.nus.cool.core.io.compression.OutputCompressor;
import com.nus.cool.core.schema.CompressType;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;


/**
 * CohortWS writes the list of users found by a cohort selection query to a file
 * COOL store the file under the version of cube queried.
 */
public class CohortWSStr implements Output {

  /**
   * A list of user id as keys in string.
   */
  private final HashSet<String> usersStrSet = new HashSet<>();

  /**
   * charset is set in table.yaml and is utf-8 by default.
   */
  Charset charset;

  public CohortWSStr(Charset charset) {
    this.charset = charset;
  }

  public CohortWSStr() {
    this.charset = StandardCharsets.UTF_8;
  }

  /**
   * Add user id (string) to local list.
   *
   * @param userId as string
   */
  public void addCubletResults(String userId) {
    usersStrSet.add(userId);
  }

  public void addCubletResults(List<String> userIds) {
    usersStrSet.addAll(userIds);
  }

  /**
   * Get number of user in a cohort.
   *
   * @return number of users
   */
  public int getNumUsers() {
    return usersStrSet.size();
  }

  @Override
  public int writeTo(DataOutput out) throws IOException {
    // convert hash set to array
    List<FieldValue> userStrArr = new ArrayList<>(usersStrSet.size()); 
    for (String s : usersStrSet) {
      userStrArr.add(ValueWrapper.of(s)); 
    }
    return OutputCompressor.writeTo(CompressType.KeyString,
      Histogram.builder().charset(charset).build(),
      userStrArr, out);
  }
}
