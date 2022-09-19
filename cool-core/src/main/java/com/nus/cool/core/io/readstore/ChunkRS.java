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
 * 
 * <p>
 * DataChunk layout
 * ---------------------------------------------
 * | chunk data | chunk header | header offset |
 * ---------------------------------------------
 * 
 * <p>
 * chunk data layout
 * -------------------------------------
 * | field 1 | field 2 | ... | field N |
 * -------------------------------------
 * 
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
   * Number of record in this chunk.
   */
  @Getter
  private int records;

  /**
   * Field array in this chunk.
   */
  private FieldRS[] fields;

  private int[] fieldOffsets;

  private TableSchema schema;

  public ChunkRS(TableSchema schema) {
    this.schema = schema;
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
    int fields = buffer.getInt();
    // Get field offsets
    this.fieldOffsets = new int[fields];
    for (int i = 0; i < fields; i++) {
      this.fieldOffsets[i] = buffer.getInt();
    }

    this.fields = new FieldRS[fields];

    // System.out.println("#Records="+this.records + ", # fields="+fields+",
    // fieldOffsets="+ Arrays.toString(fieldOffsets));

    /*
     * TODO(lingze)
     * lazy load
     * no need to load all field into memory.
     * We only load needed field
     */

    this.fields = new FieldRS[fields];
    int fieldIndex = 0;
    for (int i = 0; i < schema.getFields().size(); i++) {
      // System.out.println("Reading data chunk's field ="+i+".....");
      if (schema.isInvariantField(i)) {
        continue;
      }
      buffer.position(fieldOffsets[fieldIndex]);

      this.fields[fieldIndex++] = FieldRS.readFieldRS(buffer,
        this.schema.getField(i).getFieldType());
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

  public FieldRS getField(String fieldName) {
    return getField(schema.getDataChunkFieldID(fieldName));
  }

  public boolean isInvariantFieldByName(String name) {
    return this.schema.isInvariantField(name);
  }

  public String getUserFieldName() {
    return this.schema.getField(this.schema.getUserKeyField()).getName();
  }

  public FieldType getFieldTypeByName(String name) {
    return this.schema.getFieldType(name);
  }
}
