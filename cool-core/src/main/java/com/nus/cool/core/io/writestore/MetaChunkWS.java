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
import com.nus.cool.core.io.Output;
import com.nus.cool.core.io.compression.OutputCompressor;
import com.nus.cool.core.schema.ChunkType;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.IntegerUtil;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;

/**
 * MetaChunk write store
 * <p>
 * MetaChunk layout
 * ------------------------------------------
 * | MetaChunkData | header | header offset |
 * ------------------------------------------
 * <p>
 * MetaChunkData layout
 * -------------------------------------
 * | field 1 | field 2 | ... | field N |
 * -------------------------------------
 * <p>
 * header layout
 * ---------------------------------------
 * | ChunkType | #fields | field offsets |
 * ---------------------------------------
 * where
 * ChunkType = ChunkType.META
 * #fields = number of fields
 */
public class MetaChunkWS implements Output {

  private int offset;

  private final int userKeyIndex;

  @Getter
  private List<Integer> invariantFieldIndex = new ArrayList<>();

  @Getter
  private final MetaFieldWS[] metaFields;

  /**
   * Constructor.
   *
   * @param offset     Offset in out stream
   * @param metaFields fields type for each file of the meta chunk
   */
  public MetaChunkWS(int offset, MetaFieldWS[] metaFields, int userKeyIndex,
      List<Integer> invariantFieldIndex) {
    this.userKeyIndex = userKeyIndex;
    this.invariantFieldIndex = invariantFieldIndex;
    this.metaFields = checkNotNull(metaFields);
    checkArgument(offset >= 0 && metaFields.length > 0);
    this.offset = offset;
  }

  /**
   * MetaChunkWS Build.
   *
   * @param schema table schema created with table.yaml
   * @param offset Offset begin to write metaChunk.
   * @return MetaChunkWS instance
   */
  public static MetaChunkWS newMetaChunkWS(TableSchema schema, int offset, int userKeyIndex,
      List<Integer> invariantFieldIndex) {
    OutputCompressor compressor = new OutputCompressor();
    Charset charset = Charset.forName(schema.getCharset());

    // n: it denotes the number of columns
    int n = schema.getFields().size();
    MetaFieldWS[] metaFields = new MetaFieldWS[n];
    for (int i = 0; i < metaFields.length; i++) {
      FieldSchema fieldSchema = schema.getField(i);
      FieldType fieldType = fieldSchema.getFieldType();

      switch (fieldType) {
        case UserKey:
          metaFields[i] = new MetaUserFieldWS(fieldType, charset, compressor,
            schema.getInvariantSize());
          break;
        case AppKey:
        case Action:
        case Segment:
          metaFields[i] = new MetaHashFieldWS(fieldType, charset, compressor);
          break;
        case ActionTime:
        case Metric:
          metaFields[i] = new MetaRangeFieldWS(fieldType);
          break;
        default:
          throw new IllegalArgumentException("Invalid field type: " + fieldType);
      }
    }
    return new MetaChunkWS(offset, metaFields, userKeyIndex, invariantFieldIndex);
  }

  /**
   * Put a tuple into the meta chunk.
   *
   * @param tuple Plain data line
   */
  public void put(String[] tuple, List<FieldType> invariantType) {
    checkNotNull(tuple);
    for (int i = 0; i < tuple.length; i++) {
      if (userKeyIndex == i) {
        List<String> userData = new ArrayList<>();
        userData.add(tuple[userKeyIndex]);
        for (int j = 0; j < invariantFieldIndex.size(); j++) {
          userData.add(tuple[invariantFieldIndex.get(i)]);
        }
        ((MetaUserFieldWS) this.metaFields[i]).put(
            (String[]) userData.toArray(new String[0]), invariantType);
      } else {
        this.metaFields[i].put(tuple[i]);
      }
    }
  }

  /**
   * Complete MetaFields.
   */
  public void complete() {
    for (MetaFieldWS metaField : this.metaFields) {
      metaField.complete();
    }
  }

  /**
   * Write MetaChunkWS to out stream and return bytes written.
   *
   * @param out stream can write data to output stream
   * @return How many bytes has been written
   * @throws IOException If an I/O error occurs
   */
  @Override
  public int writeTo(DataOutput out) throws IOException {
    int bytesWritten = 0;

    // Store field offsets and write MetaFields as MetaChunkData layout
    int[] offsets = new int[this.metaFields.length];
    for (int i = 0; i < this.metaFields.length; i++) {
      offsets[i] = this.offset + bytesWritten;
      bytesWritten += this.metaFields[i].writeTo(out);
    }

    // Store header offset for MetaChunk layout
    final int headOffset = this.offset + bytesWritten;

    // 1. Write ChunkType for header layout
    out.writeByte(ChunkType.META.ordinal());
    bytesWritten++;

    // 2.1 Write fields for header layout
    out.writeInt(IntegerUtil.toNativeByteOrder(this.metaFields.length));
    bytesWritten += Ints.BYTES;

    // 2.2 Write field offsets for header layout
    for (int offset : offsets) {
      out.writeInt(IntegerUtil.toNativeByteOrder(offset));
      bytesWritten += Ints.BYTES;
    }

    // 3. Write header offset for MetaChunk layout
    out.writeInt(IntegerUtil.toNativeByteOrder(headOffset));
    bytesWritten += Ints.BYTES;
    return bytesWritten;
  }

  /**
   * Clear the stats tracking for a cublet, to process the next one afresh.
   */
  public void cleanForNextCublet() {
    for (MetaFieldWS metaField : this.metaFields) {
      metaField.cleanForNextCublet();
    }
  }

  /**
   * Write out cube meta information. 
   */
  public int writeCubeMeta(DataOutput out) throws IOException {
    this.offset = 0;
    int bytesWritten = 0;

    // Store field offsets and write MetaFields as MetaChunkData layout
    int[] offsets = new int[this.metaFields.length];
    for (int i = 0; i < this.metaFields.length; i++) {
      offsets[i] = this.offset + bytesWritten;
      bytesWritten += this.metaFields[i].writeCubeMeta(out);
    }

    // Store header offset for MetaChunk layout
    final int headOffset = this.offset + bytesWritten;

    // 1. Write ChunkType for header layout
    out.writeByte(ChunkType.META.ordinal());
    bytesWritten++;

    // 2.1 Write fields for header layout
    out.writeInt(IntegerUtil.toNativeByteOrder(this.metaFields.length));
    bytesWritten += Ints.BYTES;

    // 2.2 Write field offsets for header layout
    for (int offset : offsets) {
      out.writeInt(IntegerUtil.toNativeByteOrder(offset));
      bytesWritten += Ints.BYTES;
    }

    // 3. Write header offset for MetaChunk layout
    out.writeInt(IntegerUtil.toNativeByteOrder(headOffset));
    bytesWritten += Ints.BYTES;
    return bytesWritten;
  }

  @Override
  public String toString() {
    return "MetaChunk: " + Arrays.asList(metaFields).stream()
        .map(Object::toString).reduce((x, y) -> x + ", " + y);
  }

  /**
   * Update beginning offset.
   *
   * @param newOffset new offset to write metaChunk
   */
  public void updateBeginOffset(int newOffset) {
    this.offset = newOffset;
  }
}
