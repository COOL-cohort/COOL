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
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import java.io.DataOutput;
import java.io.IOException;

/**
 * DataChunk write store
 * <p>
 * DataChunk layout
 * ---------------------------------------------
 * | chunk data | chunk header | header offset |
 * ---------------------------------------------
 * <p>
 * chunk data layout
 * -------------------------------------
 * | field 1 | field 2 | ... | field N |
 * -------------------------------------
 * <p>
 * chunk header layout
 * ---------------------------------------------------
 * | chunk type | #records | #fields | field offsets |
 * ---------------------------------------------------
 * where
 * ChunkType == ChunkType.DATA
 * #records == number of records
 * #fields == number of fields
 */
public class DataChunkWS implements Output {

  /**
   * Chunk beginning offset, don't update.
   */
  private final int chunkBeginOffset;

  /**
   * Number of records.
   */
  private int recordCount;

  /**
   * Fields in data chunk.
   */
  private final DataFieldWS[] dataFields;

  /**
   * Constructor.
   *
   * @param offset Offset in out stream
   * @param fields fields for this data chunk
   */
  public DataChunkWS(int offset, DataFieldWS[] fields) {
    this.dataFields = checkNotNull(fields);
    checkArgument(offset >= 0 && fields.length > 0);
    this.chunkBeginOffset = offset;
  }

  /**
   * Data Chunk Builder.
   *
   * @param schema     tableSchema
   * @param metaFields MetaFields
   * @param offset     Offset in out stream
   * @return DataChunkWs instance
   */
  public static DataChunkWS newDataChunk(TableSchema schema, MetaFieldWS[] metaFields, int offset)
      throws IllegalArgumentException {
    // OutputCompressor compressor = new OutputCompressor();
    int numOfFields = schema.count();
    // data chunk fields.
    // don't have to maintain dataField for invairant Field
    DataFieldWS[] fields = new DataFieldWS[numOfFields];

    for (int i = 0; i < numOfFields; i++) {

      FieldType fieldType = schema.getField(i).getFieldType();
      switch (fieldType) {
        case UserKey:
        case AppKey:
        case Action:
        case Segment:
          if (!(metaFields[i] instanceof MetaHashFieldWS)) {
            throw new IllegalArgumentException("Mismatch in meta and data hash field.");
          }
          MetaHashFieldWS ws = (MetaHashFieldWS) metaFields[i];
          if (schema.isInvariantField(i)) {
            fields[i] = new DataInvariantHashFieldWS(fieldType, ws);
          } else {
            fields[i] = new DataHashFieldWS(fieldType, ws, schema.getField(i).isPreCal());
          }
          break;
        case ActionTime:
        case Metric:
        case Float:
          if (schema.isInvariantField(i)) {
            fields[i] = new DataInvariantRangeFieldWS(fieldType);
          } else {
            fields[i] = new DataRangeFieldWS(fieldType);
          }
          break;
        default:
          throw new IllegalArgumentException("Unsupported FieldType: " + fieldType);
      }
    }
    return new DataChunkWS(offset, fields);
  }

  /**
   * Put a tuple into the chunk.
   *
   * @param tuple plain data
   * @throws IOException If an I/O error occurs
   */
  public void put(FieldValue[] tuple) throws IOException {
    this.recordCount++;
    for (int i = 0; i < tuple.length; i++) {
      this.dataFields[i].put(tuple[i]);
    }
  }

  /**
   * Write DataChunkWS to out stream and return bytes written
   * 1. Write field
   * 2. Write header [chunkType, #Records, #Fields]
   *
   * @param out stream can write data to output stream
   * @return bytes written
   * @throws IOException If an I/O error occurs
   */
  @Override
  public int writeTo(DataOutput out) throws IOException {
    // how many byte has been written.
    int bytesWritten = 0;
    int[] offsets = new int[this.dataFields.length];

    // Calculate offset of field
    // 1. Write fields
    for (int i = 0; i < this.dataFields.length; i++) {
      offsets[i] = this.chunkBeginOffset + bytesWritten;
      bytesWritten += this.dataFields[i].writeTo(out);
    }

    // 2. Write header of the Data Chunk.
    // Calculate offset of header
    final int chunkHeadOff = this.chunkBeginOffset + bytesWritten;
    // 2.1 Write chunkType (D ATA)'s position 1 Byte to store the ChunkType
    out.write(ChunkType.DATA.ordinal());
    bytesWritten++;
    // 2.2 Write #records
    out.writeInt(this.recordCount);
    bytesWritten += Ints.BYTES;
    // 2.3 Write #fields
    out.writeInt(this.dataFields.length);
    bytesWritten += Ints.BYTES;
    // 2.4 Write field offsets
    for (int offset : offsets) {
      out.writeInt(offset);
      bytesWritten += Ints.BYTES;
    }

    // 3. Write header offset
    out.writeInt(chunkHeadOff);
    bytesWritten += Ints.BYTES;
    return bytesWritten;
  }
}
