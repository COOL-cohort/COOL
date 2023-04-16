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

import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.RangeField;
import com.nus.cool.core.schema.FieldType;
import java.io.DataOutput;
import java.io.IOException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Range MetaField write store.
 * 
 * <p>
 * Data Layout
 * -------------
 * | min | max |
 * -------------
 */
@RequiredArgsConstructor
public class MetaRangeFieldWS implements MetaFieldWS {

  private final FieldType fieldType;

  @Getter
  private RangeField min = null;

  @Getter
  private RangeField max = null;

  private RangeField cubeMax = null;

  private RangeField cubeMin = null;

  @Override
  public void put(FieldValue[] tuple, int idx) throws IllegalArgumentException {
    if (!(tuple[idx] instanceof RangeField)) {
      throw new IllegalArgumentException(
        "Illegal argument for MetaRangeFieldWS (RangeField required).");
    }
    RangeField v = (RangeField) tuple[idx];
    min = ((min == null) || (min.compareTo(v) > 0)) ? v : min;
    max = ((max == null) || (max.compareTo(v) < 0)) ? v : max;
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
    if (min == null) {
      return; // empty
    }
    cubeMin = ((cubeMin == null) || (cubeMin.compareTo(min) > 0)) ? min : cubeMin;
    cubeMax = ((cubeMax == null) || (cubeMax.compareTo(max) < 0)) ? max : cubeMax;
  }

  @Override
  public void cleanForNextCublet() {
    this.max = null;
    this.min = null;
  }

  @Override
  public int writeTo(DataOutput out) throws IOException {
    int bytesWritten = 0;
    switch (this.fieldType) {
      case Float:
        out.writeFloat(this.min.getFloat());
        out.writeFloat(this.max.getFloat());
        bytesWritten += 2 * Float.BYTES;
        break;
      case Metric:
      case ActionTime:
        out.writeInt(this.min.getInt());
        out.writeInt(this.max.getInt());
        bytesWritten += 2 * Integer.BYTES;
        break;
      default:
        throw new IllegalArgumentException("Invalid field type: " + this.fieldType);   
    }
    return bytesWritten;
  }

  @Override
  public int writeCubeMeta(DataOutput out) throws IOException {
    int bytesWritten = 0;
    switch (this.fieldType) {
      case Float:
        out.writeFloat(this.cubeMin.getFloat());
        out.writeFloat(this.cubeMax.getFloat());
        bytesWritten += 2 * Float.BYTES;
        break;
      case Metric:
      case ActionTime:
        out.writeInt(this.cubeMin.getInt());
        out.writeInt(this.cubeMax.getInt());
        bytesWritten += 2 * Integer.BYTES;
        break;
      default:
        throw new IllegalArgumentException("Invalid field type: " + this.fieldType);   
    }
    return bytesWritten;
  }

  @Override
  public String toString() {
    return "RangeMetaField(min,max): (" + min + ", " + max + ")";
  }
}
