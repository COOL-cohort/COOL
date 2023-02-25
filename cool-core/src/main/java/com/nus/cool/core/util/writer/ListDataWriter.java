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

package com.nus.cool.core.util.writer;

import static com.google.common.base.Preconditions.checkNotNull;

import com.nus.cool.core.field.FieldValue;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * ListDataWriter write the serialized representation of the records to a list.
 *  (It is currently used to generate the responses of cohort exploration in query server) 
 */
public class ListDataWriter implements DataWriter {

  // It stores the output data
  private List<String> out;

  public ListDataWriter(List<String> out) {
    this.out = checkNotNull(out);
  }
  
  @Override
  public boolean initialize() throws IOException {
    return true;
  }

  @Override
  public boolean add(FieldValue[] tuple) throws IOException {
    String[] fields = new String[tuple.length];        
    for (int i = 0; i < tuple.length; i++) {
      fields[i] = tuple[i].getString();
    }
    out.add(Arrays.toString(fields));
    return false;
  }

  @Override
  public void finish() throws IOException {
    // no-op
  }

  @Override
  public void close() throws IOException {
    // no-op    
  }
}
