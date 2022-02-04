/*
 * Copyright 2020 Cool Squad Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nus.cool.core.io.readstore;

import com.nus.cool.core.io.Input;
import com.nus.cool.core.schema.ChunkType;
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
 *
 * @author zhongle
 * @version 0.1
 * @since 0.1
 */
public class ChunkRS implements Input {

  /**
   * number of record in this chunk
   */
  @Getter
  private int records;

  /**
   * field array in this chunk
   */
  private FieldRS[] fields;

  private TableSchema schema;

  public ChunkRS(TableSchema schema) {
    this.schema = schema;
  }

  @Override
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
    int[] fieldOffsets = new int[fields];
      for (int i = 0; i < fields; i++) {
          fieldOffsets[i] = buffer.getInt();
      }

    this.fields = new FieldRS[fields];
    for (int i = 0; i < fields; i++) {
      buffer.position(fieldOffsets[i]);
      FieldRS field = new CoolFieldRS();
      field.readFromWithFieldType(buffer, this.schema.getField(i).getFieldType());
      this.fields[i] = field;
    }
  }

  public FieldRS getField(int i) {
    return this.fields[i];
  }

  public FieldRS getField(String fieldName) {
    return getField(schema.getFieldID(fieldName));
  }

}
