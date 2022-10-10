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
import com.nus.cool.core.io.Input;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.InputVectorFactory;
import com.nus.cool.core.io.storevector.LZ4InputVector;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * rewrite of CohortRS for the updated persistent cohort format
 */
public class CohortRSStr implements Input {

  /**
   * A list of user values.
   */
  private List<String> values = null;

  /**
   * default is StandardCharsets.UTF_8
   */
  private final Charset charset;

  public CohortRSStr(Charset charset){
    this.charset = charset;
  }

  public CohortRSStr(){
    this.charset = StandardCharsets.UTF_8;
  }

  /**
   * Get the userid list
   * @return user id list
   * @throws IllegalStateException IllegalStateException
   * @throws IllegalArgumentException IllegalArgumentException
   */
  public List<String> getUsers()
      throws IllegalStateException, IllegalArgumentException {
    return values;
  }

  @Override
  public void readFrom(ByteBuffer buffer) {
    InputVector vec = InputVectorFactory.readFrom(buffer);
    LZ4InputVector value_vec = (LZ4InputVector) vec;
    int value_count = value_vec.size();
    values = new ArrayList<>(value_count);
    for (int i = 0; i < value_count; i++) {
      String value = value_vec.getString(i, this.charset);
      values.add(value);
    }
  }

}
