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

package com.nus.cool.core.io.compression;

import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.schema.Codec;
import com.nus.cool.core.schema.CompressType;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Serialize and compress a list of field values to output.
 */
public class OutputCompressor {

  /**
   * Compress the data and write to a data output.
   */
  public static int writeTo(CompressType type, Histogram h,
      List<? extends FieldValue> vec, DataOutput out)
      throws IOException {
    int bytesWritten = 0;
    // 1. select a compressor type
    Codec codec = CompressorAdviser.advise(type, h);
    // 2. create compressor instance according to the type
    Compressor compressor = CompressorFactory.newCompressor(codec, h);
    // 3. compress it and record output to compressed array
    CompressorOutput compressed = compressor.compress(vec);

    // Write compressor type
    out.writeByte(codec.ordinal());
    bytesWritten++;
    // Write compressed data
    out.write(compressed.getBuf(), 0, compressed.getLen());
    bytesWritten += compressed.getLen();
    return bytesWritten;
  }
}
