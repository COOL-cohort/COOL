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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Maps;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.InputVectorFactory;
import com.nus.cool.core.io.storevector.LZ4InputVector;
import com.nus.cool.core.schema.FieldType;
import com.rabinhash.RabinHashFunction32;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;

public class HashMetaFieldRS implements MetaFieldRS {

  protected static final RabinHashFunction32 rhash = RabinHashFunction32.DEFAULT_HASH_FUNCTION;

  protected Charset charset;

  protected FieldType fieldType;

  protected InputVector fingerVec;

  protected InputVector globalIDVec;

  protected InputVector valueVec;

  // inverse map from global id to the offset in values.
  //  only populated once when getString is called to retrieve from valueVec
  protected Map<Integer, Integer> id2offset;

  public HashMetaFieldRS(Charset charset) {
    this.charset = checkNotNull(charset);
  }

  @Override
  public FieldType getFieldType() {
    return this.fieldType;
  }

  @Override
  public int find(String key) {
    int globalIDIdx = this.fingerVec.find(rhash.hash(key));
    return this.globalIDVec.get(globalIDIdx);
  }

  @Override
  public int count() {
    return this.fingerVec.size();
  }

  @Override
  public String getString(int i) {
    if (this.id2offset == null) {
      this.id2offset = Maps.newHashMap();
      // lazily populate the inverse index only once
      for (int j = 0; j < this.globalIDVec.size(); j++) {
        this.id2offset.put(this.globalIDVec.get(j), j);
      }
    }
    return ((LZ4InputVector) this.valueVec)
      .getString(this.id2offset.get(i), this.charset);
  }

  @Override
  public int getMaxValue() {
    return this.count() - 1;
  }

  @Override
  public int getMinValue() {
    return 0;
  }

  @Override
  public void readFromWithFieldType(ByteBuffer buffer, FieldType fieldType) {
    this.fieldType = fieldType;
    this.fingerVec = InputVectorFactory.readFrom(buffer);
    this.globalIDVec = InputVectorFactory.readFrom(buffer);
      if (this.fieldType == FieldType.Action || this.fieldType == FieldType.Segment
          || this.fieldType == FieldType.UserKey) {
          this.valueVec = InputVectorFactory.readFrom(buffer);
      }
  }

  @Override
  public void readFrom(ByteBuffer buffer) {
    FieldType fieldType = FieldType.fromInteger(buffer.get());
    this.readFromWithFieldType(buffer, fieldType);
  }

  public String[] getGidMap(){
    // Can store it and reuse ret (suggestion)
    String[] ret = new String[this.count()];
    LZ4InputVector strlist = (LZ4InputVector) this.valueVec;
    for(int i = 0; i < ret.length; i++){
      ret[this.globalIDVec.get(i)] = strlist.getString(i, this.charset);
    }
    return ret;
  }
}
