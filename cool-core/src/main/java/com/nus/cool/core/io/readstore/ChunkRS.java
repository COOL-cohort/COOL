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

import com.nus.cool.core.io.Input;
import com.nus.cool.core.schema.ChunkType;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import java.nio.ByteBuffer;
import lombok.Getter;

/**
 * DataChunk read store
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
public class ChunkRS implements Input {

  /**
   * number of record in this chunk.
   */
  @Getter
  private int records;

  /**
   * field array in this chunk.
   */
  private FieldRS[] fields;

  private int[] fieldOffsets;

  private TableSchema tableSchema;

  private MetaChunkRS metaChunkRS;

  public ChunkRS(TableSchema tableSchema, MetaChunkRS metaChunkRS) {
    this.tableSchema = tableSchema;
    this.metaChunkRS = metaChunkRS;
  }

  public int records() {
    return this.records;
  }

  @Override
  // read the dataChunk
  public void readFrom(ByteBuffer buffer) {
    // Get chunkType
    ChunkType chunkType = ChunkType.fromInteger(buffer.get());
    if (chunkType != ChunkType.DATA) {
      throw new IllegalStateException("Expect DataChunk, but reads: " + chunkType);
    }

    // Get #records
    this.records = buffer.getInt();
    // Get #fields
    int fieldCount = buffer.getInt();
    // Get field offsets
    this.fieldOffsets = new int[fieldCount];
    for (int i = 0; i < fieldCount; i++) {
      this.fieldOffsets[i] = buffer.getInt();
    }

    MetaUserFieldRS userMetaField = (MetaUserFieldRS) this.metaChunkRS.getMetaField(
        tableSchema.getUserKeyFieldName());

    // initialized UserDataField first, it will become args for invariant field
    DataHashFieldRS userDataField = new DataHashFieldRS();

    this.fields = new FieldRS[fieldCount];
    for (int i = 0; i < tableSchema.count(); i++) {
      buffer.position(fieldOffsets[i]);
      FieldType fieldType = tableSchema.getFieldType(i);

      if (i == tableSchema.getUserKeyFieldIdx()) {
        userDataField.readFromWithFieldType(buffer, fieldType);
        this.fields[i] = userDataField;
        continue;
      }

      if (tableSchema.isInvariantField(i)) {
        int invariantIdx = tableSchema.getInvariantFieldFlagMap()[i];
        // invariant_idx != -1;
        this.fields[i] = new DataInvariantFieldRS(
            fieldType, invariantIdx, userMetaField, userDataField);
      } else if (FieldType.isHashType(fieldType)) {
        this.fields[i] = DataHashFieldRS.readFrom(buffer, fieldType);
      } else {
        // range field
        this.fields[i] = DataRangeFieldRS.readFrom(buffer, fieldType);
      }
    }
  }

  /**
   * Get the field information according to index.
   *
   * @param i index of field
   */
  public FieldRS getField(int i) {
    return this.fields[i];
  }

  /**
   * Get the filed information according to field name.
   *
   * @param fieldName fileName
   * @return fields
   */
  public FieldRS getField(String fieldName) {
    return getField(tableSchema.getFieldID(fieldName));
  }

}
