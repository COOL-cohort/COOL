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

import com.google.common.primitives.Ints;
import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.RangeField;
import com.nus.cool.core.io.compression.Histogram;
import com.nus.cool.core.io.compression.OutputCompressor;
import com.nus.cool.core.schema.Codec;
import com.nus.cool.core.schema.CompressType;
import com.nus.cool.core.schema.FieldType;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Range index, used to store chunk data for two fieldTypes, including
 * ActionTime, Metric.
 * <p>
 * Data layout
 * ------------------------------
 * | codec | min | max | values |
 * ------------------------------
 * where
 * min = min of the values
 * max = max of the values
 * values = column data (compressed)
 */
public class DataRangeFieldWS implements DataFieldWS {

  private final FieldType fieldType;

  private final List<FieldValue> values = new LinkedList<>();

  private RangeField min;
  private RangeField max;

  /**
   * Create a write store of int field for data chunk.
   */
  public DataRangeFieldWS(FieldType fieldType) {
    this.fieldType = fieldType;
  }

  @Override
  public FieldType getFieldType() {
    return this.fieldType;
  }

  /**
   * insert value.
   *
   * @param tupleValue value
   * @throws IOException when string convert fail
   */
  @Override
  public void put(FieldValue tupleValue) throws IllegalArgumentException {
    if (!(tupleValue instanceof RangeField)) {
      throw new IllegalArgumentException(
        "Invalid FieldValue for DataRangeFieldWS (RangeField required).");
    }
    RangeField v = (RangeField) tupleValue;
    values.add(v);
    min = ((min == null) || (min.compareTo(v) > 0)) ? v : min;
    max = ((max == null) || (max.compareTo(v) < 0)) ? v : max;
  }

  @Override
  public int writeTo(DataOutput out) throws IOException {
    int bytesWritten = 0;

    // Write codec
    out.write(Codec.Range.ordinal());
    bytesWritten++;
    // Write min value
    out.writeInt(min.getInt());
    bytesWritten += Ints.BYTES;
    // Write max value
    out.writeInt(max.getInt());
    bytesWritten += Ints.BYTES;

    // Write values, i.e. the data within the column
    int count = values.size();
    // int rawSize = count * Ints.BYTES;
    Histogram hist = Histogram.builder()
        .min(min)
        .max(max)
        .numOfValues(count)
        .build();
    if (this.fieldType == FieldType.Float) {
      bytesWritten += OutputCompressor.writeTo(CompressType.Float, hist, values, out);
    } else {
      bytesWritten += OutputCompressor.writeTo(CompressType.ValueFast, hist, values, out);
    }
    return bytesWritten;
  }
}
