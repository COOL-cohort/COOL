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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Maps;
import com.nus.cool.core.io.Input;
import com.nus.cool.core.schema.ChunkType;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;
import lombok.Getter;

/**
 * MetaChunk read store
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
public class MetaChunkRS implements Input {

  /**
   * TableSchema for this meta chunk.
   */
  @Getter
  private TableSchema schema;

  /**
   * charset defined in table schema.
   */
  private Charset charset;

  /**
   * stored data byte buffer.
   */
  private ByteBuffer buffer;

  /**
   * offsets for fields in this meta chunk.
   */
  private int[] fieldOffsets;

  private Map<Integer, MetaFieldRS> fields = Maps.newHashMap();

  public MetaChunkRS(TableSchema schema) {
    this.schema = checkNotNull(schema);
    this.charset = Charset.forName(this.schema.getCharset());
  }

  @Override
  public void readFrom(ByteBuffer buffer) {
    // Read header
    // Get chunk type
    this.buffer = checkNotNull(buffer);
    ChunkType chunkType = ChunkType.fromInteger(this.buffer.get());
    if (chunkType != ChunkType.META) {
      throw new IllegalStateException("Expect MetaChunk, but reads: " + chunkType);
    }

    // Get #fields
    int fields = this.buffer.getInt();
    // Get field offsets
    this.fieldOffsets = new int[fields];
    for (int i = 0; i < fields; i++) {
      fieldOffsets[i] = this.buffer.getInt();
    }
    // Fields are loaded only if they are called
  }

  /**
   * Get i-th MetaField and load it.
   *
   * @param i index of this metafield
   * @param type type of this metafield
   * @return MetaFieldRS
   */
  public synchronized MetaFieldRS getMetaField(int i, FieldType type) {
    // Return the meta field if it had been read before
    if (this.fields.containsKey(i)) {
      return this.fields.get(i);
    }
    // Read the meta field from the buffer if it is called at the first time
    int fieldOffset = this.fieldOffsets[i];
    this.buffer.position(fieldOffset);
    MetaFieldRS metaField = null;

    switch (type) {
      case UserKey:
        metaField = new MetaUserFieldRS(this, this.charset);
        break;
      case AppKey:
      case Action:
      case Segment:
        metaField = new MetaHashFieldRS(this.charset);
        break;
      case Metric:
      case Float:
      case ActionTime:
        metaField = new MetaRangeFieldRS();
        break;
      default:
        throw new IllegalArgumentException("Unexpected FieldType: " + type);
    }
    metaField.readFromWithFieldType(this.buffer, type);
    this.fields.put(i, metaField);
    return metaField;
  }

  /**
   * Get the MetaField according to the fieldName in tableSchema.
   *
   * @param fieldName fieldName
   * @return MetaFieldRS
   */
  public MetaFieldRS getMetaField(String fieldName) {
    int id = this.schema.getFieldID(fieldName);
    FieldType type = this.schema.getFieldType(fieldName);
    return (id < 0 || id >= this.fieldOffsets.length) ? null : this.getMetaField(id, type);
  }

  public MetaFieldRS getMetaField(int idx) {
    FieldType type = this.schema.getFieldType(idx);
    return (idx < 0 || idx >= this.fieldOffsets.length) ? null : this.getMetaField(idx, type);
  }
}
