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
package com.nus.cool.core.io.writestore;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.primitives.Ints;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.util.IntegerUtil;
import com.nus.cool.core.util.converter.DayIntConverter;

import lombok.Getter;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Range MetaField write store
 * <p>
 * Data Layout
 * -------------
 * | min | max |
 * -------------
 */
public class MetaRangeFieldWS implements MetaFieldWS {

  private final FieldType fieldType;

  @Getter
  private int min;

  @Getter
  private int max;

  private int cubeMax;

  private int cubeMin;

  public MetaRangeFieldWS(FieldType type) {
    this.fieldType = type;
    this.min = Integer.MAX_VALUE;
    this.max = Integer.MIN_VALUE;
    this.cubeMax = this.max;
    this.cubeMin = this.min;
  }


  @Override
  public void put(String v) {
    v = checkNotNull(v);
    switch (this.fieldType) {
      case Metric:
        this.min = Math.min(this.min, Integer.parseInt(v));
        this.max = Math.max(this.max, Integer.parseInt(v));
        break;
      case ActionTime:
        DayIntConverter converter = new DayIntConverter();
        this.min = Math.min(this.min, converter.toInt(v));
        this.max = Math.max(this.max, converter.toInt(v));
        break;
      default:
        throw new IllegalArgumentException("Unable to index: " + this.fieldType);
    }
    checkArgument(this.min <= this.max);
  }

  @Override
  public int find(String v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int count() {
    throw new UnsupportedOperationException();
  }

  @Override
  public FieldType getFieldType() {
    return this.fieldType;
  }

  @Override
  public void complete() {
    if (min > max) return; // empty
    this.cubeMax = Math.max(this.max, this.cubeMax);
    this.cubeMin = Math.min(this.min, this.cubeMin);
  }

  @Override
  public void cleanForNextCublet() {
    this.max = Integer.MIN_VALUE;
    this.min = Integer.MAX_VALUE;
  }

  // guoyu0724 outdated
  // @Override
  // public void update(String v) {
  //   throw new UnsupportedOperationException("Doesn't support update now");
  // }

  @Override
  public int writeTo(DataOutput out) throws IOException {
    int bytesWritten = 0;
    out.writeInt(IntegerUtil.toNativeByteOrder(this.min));
    out.writeInt(IntegerUtil.toNativeByteOrder(this.max));
    bytesWritten += 2 * Ints.BYTES;
    return bytesWritten;
  }
  
  @Override
  public int writeCubeMeta(DataOutput out) throws IOException {
    int bytesWritten = 0;
    out.writeInt(IntegerUtil.toNativeByteOrder(this.cubeMin));
    out.writeInt(IntegerUtil.toNativeByteOrder(this.cubeMax));
    bytesWritten += 2 * Ints.BYTES;
    return bytesWritten;
  }

  @Override
  public String toString() {
    return "RangeMetaField(min,max): (" + min + ", " + max + ")";
  }
}
