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
import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.io.Output;
import com.nus.cool.core.schema.ChunkType;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
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

  // private final int userKeyIndex;

  // @Getter
  // private List<Integer> invariantFieldIndex = new ArrayList<>();

  @Getter
  private final MetaFieldWS[] metaFields;

  @Getter
  private TableSchema tableSchema;

  /**
   * Constructor.
   *
   * @param offset     Offset in out stream
   * @param metaFields fields type for each file of the meta chunk
   */
  public MetaChunkWS(int offset, MetaFieldWS[] metaFields, TableSchema schema) {
    this.tableSchema = schema;
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
  public static MetaChunkWS newMetaChunkWS(TableSchema schema, int offset) {
    Charset charset = Charset.forName(schema.getCharset());

    // n: it denotes the number of columns
    int n = schema.getFields().size();
    MetaFieldWS[] metaFields = new MetaFieldWS[n];
    MetaChunkWS metaChunk = new MetaChunkWS(offset, metaFields, schema);
    for (int i = 0; i < metaFields.length; i++) {
      FieldSchema fieldSchema = schema.getField(i);
      FieldType fieldType = fieldSchema.getFieldType();

      switch (fieldType) {
        case UserKey:
          metaFields[i] = new MetaUserFieldWS(fieldType, charset, metaChunk);
          break;
        case AppKey:
        case Action:
        case Segment:
          metaFields[i] = new MetaHashFieldWS(fieldType, charset);
          break;
        case ActionTime:
        case Float:
        case Metric:
          metaFields[i] = new MetaRangeFieldWS(fieldType);
          break;
        default:
          throw new IllegalArgumentException("Invalid field type: " + fieldType);
      }
    }
    return metaChunk;
  }

  /**
   * Put a tuple into the meta chunk.
   *
   * @param tuple Plain data line
   */
  public void put(FieldValue[] tuple) {
    checkNotNull(tuple);
    checkArgument(tuple.length == this.tableSchema.getFields().size(),
        "input tuple's size is not equal to table schema's size");
    int userKeyIdx = this.tableSchema.getUserKeyFieldIdx();
    for (int i = 0; i < tuple.length; i++) {
      if (i == userKeyIdx) {
        continue;
      }
      this.metaFields[i].put(tuple, i);
    }
    // the user key meta field should be updated at last 
    // as it needs the globalId of values from other meta field
    this.metaFields[userKeyIdx].put(tuple, userKeyIdx);
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
    out.writeInt(this.metaFields.length);
    bytesWritten += Ints.BYTES;

    // 2.2 Write field offsets for header layout
    for (int offset : offsets) {
      out.writeInt(offset);
      bytesWritten += Ints.BYTES;
    }

    // 3. Write header offset for MetaChunk layout
    out.writeInt(headOffset);
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
   * write CubeMeta (complete should be called first).
   *
   * @param out dataoutput
   * @return success or fail
   *
   * @throws IOException write error
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
    out.writeInt(this.metaFields.length);
    bytesWritten += Ints.BYTES;

    // 2.2 Write field offsets for header layout
    for (int offset : offsets) {
      out.writeInt(offset);
      bytesWritten += Ints.BYTES;
    }

    // 3. Write header offset for MetaChunk layout
    out.writeInt(headOffset);
    bytesWritten += Ints.BYTES;
    return bytesWritten;
  }

  @Override
  public String toString() {
    return "MetaChunk: " + Arrays.asList(metaFields).stream()
        .map(Object::toString).reduce((x, y) -> x + ", " + y);
  }

  /**
   * Update beginning offset to write.
   *
   * @param newOffset new offset to write metaChunk
   */
  public void updateBeginOffset(int newOffset) {
    this.offset = newOffset;
  }
}
