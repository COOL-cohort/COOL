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
import com.nus.cool.core.io.DataInputBuffer;
import com.nus.cool.core.io.DataOutputBuffer;
import com.nus.cool.core.io.compression.Histogram;
import com.nus.cool.core.io.compression.OutputCompressor;
import com.nus.cool.core.schema.Codec;
import com.nus.cool.core.schema.CompressType;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.util.ArrayUtil;
import com.nus.cool.core.util.IntegerUtil;
import com.nus.cool.core.util.converter.DayIntConverter;
import java.io.DataOutput;
import java.io.IOException;

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
public class RangeFieldWS implements FieldWS {

  /**
   * Field index to get data from tuple
   */
  private int i;

  private FieldType fieldType;

  private DataOutputBuffer buffer = new DataOutputBuffer();

  private OutputCompressor compressor;

  public RangeFieldWS(FieldType fieldType, int i, OutputCompressor compressor) {
    checkArgument(i >= 0);
    this.i = i;
    this.fieldType = fieldType;
    this.compressor = checkNotNull(compressor);
  }

  @Override
  public FieldType getFieldType() {
    return this.fieldType;
  }

  /**
   * UnitTest insert data
   * 
   * @param item
   * @throws IOException
   */
  public void put(String item) throws IOException {
    if (this.fieldType == FieldType.ActionTime) {
      DayIntConverter converter = new DayIntConverter();
      this.buffer.writeInt(converter.toInt(item));
    } else {
      this.buffer.writeInt(Integer.parseInt(item));
    }
  }

  @Override
  public void put(String[] tuple) throws IOException {
    this.put(tuple[this.i]);
  }

  @Override
  public int writeTo(DataOutput out) throws IOException {
    int bytesWritten = 0;
    int[] key = { Integer.MAX_VALUE, Integer.MIN_VALUE };
    // key[0] Min Key[1] Max
    int[] value = new int[this.buffer.size() / Ints.BYTES];

    // Read column data
    try (DataInputBuffer input = new DataInputBuffer()) {
      input.reset(this.buffer);
      for (int i = 0; i < value.length; i++) {
        value[i] = input.readInt();
        key[0] = value[i] < key[0] ? value[i] : key[0];
        key[1] = value[i] < key[1] ? key[1] : value[i];
      }
    }

    // Write codec
    out.write(Codec.Range.ordinal());
    bytesWritten++;
    // Write min value
    out.writeInt(IntegerUtil.toNativeByteOrder(key[0]));

    bytesWritten += Ints.BYTES;
    // Write max value
    out.writeInt(IntegerUtil.toNativeByteOrder(key[1]));
    bytesWritten += Ints.BYTES;

    // Write values, i.e. the data within the column
    int count = value.length;
    int rawSize = count * Ints.BYTES;
    Histogram hist = Histogram.builder()
        .min(key[0])
        .max(key[1])
        .numOfValues(count)
        .rawSize(rawSize)
        .type(CompressType.ValueFast)
        .build();
    this.compressor.reset(hist, value, 0, value.length);
    bytesWritten += this.compressor.writeTo(out);
    return bytesWritten;
  }
}
