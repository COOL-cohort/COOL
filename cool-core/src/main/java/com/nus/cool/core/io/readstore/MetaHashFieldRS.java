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

import com.nus.cool.core.field.HashField;
import com.nus.cool.core.field.RangeField;
import com.nus.cool.core.io.storevector.HashFieldInputVector;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.InputVectorFactory;
import com.nus.cool.core.schema.FieldType;
import com.rabinhash.RabinHashFunction32;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Optional;

/**
 * Meta HashField ReadStore.
 */
public class MetaHashFieldRS implements MetaFieldRS {

  protected static final RabinHashFunction32 rhash = RabinHashFunction32.DEFAULT_HASH_FUNCTION;

  protected Charset charset;

  protected FieldType fieldType;

  protected InputVector<Integer> fingerVec;

  protected InputVector<Integer> globalIDVec;

  protected HashFieldInputVector valueVec;

  public MetaHashFieldRS(Charset charset) {
    this.charset = checkNotNull(charset);
  }

  @Override
  public FieldType getFieldType() {
    return this.fieldType;
  }

  @Override
  public int find(String key) {
    int globalIDIdx = this.fingerVec.find(rhash.hash(key));
    return globalIDIdx == -1 ? -1 : this.globalIDVec.get(globalIDIdx);
  }

  @Override
  public int count() {
    return this.fingerVec.size();
  }

  @Override
  public Optional<HashField> get(int i) {
    // return this.valueVec.get(i);
    return (i < count()) ? Optional.of(this.valueVec.getValue(i)) : Optional.empty();
  }

  @Override
  public RangeField getMaxValue() {
    throw new UnsupportedOperationException();
  }

  @Override
  public RangeField getMinValue() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readFromWithFieldType(ByteBuffer buffer, FieldType fieldType)
      throws IllegalArgumentException {
    this.fieldType = fieldType;
    this.fingerVec = InputVectorFactory.genIntInputVector(buffer);
    this.globalIDVec = InputVectorFactory.genIntInputVector(buffer);
    if (this.fieldType == FieldType.Action || this.fieldType == FieldType.Segment
        || this.fieldType == FieldType.AppKey) {
      this.valueVec = InputVectorFactory.genHashFieldInputVector(buffer, charset);
    }
  }

  @Override
  public void readFrom(ByteBuffer buffer) {
    FieldType fieldType = FieldType.fromInteger(buffer.get());
    this.readFromWithFieldType(buffer, fieldType);
  }
}
