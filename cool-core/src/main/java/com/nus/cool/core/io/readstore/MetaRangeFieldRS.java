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

import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.RangeField;
import com.nus.cool.core.field.ValueWrapper;
import com.nus.cool.core.schema.FieldType;
import java.nio.ByteBuffer;
import java.util.Optional;


/**
 * Read store of meta of a int field in meta chunks.
 */
public class MetaRangeFieldRS implements MetaFieldRS {

  private FieldType fieldType;

  private RangeField min; // maybe better to use RangeField

  private RangeField max;

  @Override
  public FieldType getFieldType() {
    return this.fieldType;
  }

  @Override
  public int find(String key) {
    return 0;
  }

  @Override
  public int count() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<FieldValue> get(int i) {
    return Optional.empty();
  }

  @Override
  public RangeField getMaxValue() {
    return this.max;
  }

  @Override
  public RangeField getMinValue() {
    return this.min;
  }

  @Override
  public void readFromWithFieldType(ByteBuffer buffer, FieldType fieldType) {
    this.fieldType = fieldType;
    if (fieldType == FieldType.Float) {
      this.min = ValueWrapper.of(buffer.getFloat());
      this.max = ValueWrapper.of(buffer.getFloat());
    } else {
      this.min = ValueWrapper.of(buffer.getInt());
      this.max = ValueWrapper.of(buffer.getInt());
    }
  }

  @Override
  public void readFrom(ByteBuffer buffer) {
    FieldType fieldType = FieldType.fromInteger(buffer.get());
    this.readFromWithFieldType(buffer, fieldType);
  }
}
