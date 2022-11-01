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

import com.nus.cool.core.schema.FieldType;
import java.nio.ByteBuffer;


/**
 * Meta RangeField ReadStore.
 */
public class MetaRangeFieldRS implements MetaFieldRS {

  private FieldType fieldType;

  private int min;

  private int max;

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
  public String getString(int i) {
    return null;
  }

  @Override
  public int getMaxValue() {
    return this.max;
  }

  @Override
  public int getMinValue() {
    return this.min;
  }

  @Override
  public void readFromWithFieldType(ByteBuffer buffer, FieldType fieldType) {
    this.fieldType = fieldType;
    this.min = buffer.getInt();
    this.max = buffer.getInt();
  }

  @Override
  public void readFrom(ByteBuffer buffer) {
    FieldType fieldType = FieldType.fromInteger(buffer.get());
    this.readFromWithFieldType(buffer, fieldType);
  }
}