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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Maps;
import com.nus.cool.core.io.Input;
import com.nus.cool.core.schema.ChunkType;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;

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
 *
 * @author zhongle
 * @version 0.1
 * @since 0.1
 */
public class MetaChunkRS implements Input {

  /**
   * TableSchema for this meta chunk
   */
  private TableSchema schema;

  /**
   * charset defined in table schema
   */
  private Charset charset;

  /**
   * stored data byte buffer
   */
  private ByteBuffer buffer;

  /**
   * offsets for fields in this meta chunk
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
  }

  public synchronized MetaFieldRS getMetaField(int i, FieldType type) {
      if (this.fields.containsKey(i)) {
          return this.fields.get(i);
      }

    int fieldOffset = this.fieldOffsets[i];
    this.buffer.position(fieldOffset);
    MetaFieldRS metaField = null;
    switch (type) {
      case AppKey:
      case UserKey:
      case Action:
      case Segment:
        metaField = new HashMetaFieldRS(this.charset);
        break;
      case Metric:
      case ActionTime:
        metaField = new RangeMetaFieldRS();
        break;
      default:
        throw new IllegalArgumentException("Unexpected FieldType: " + type);
    }
    metaField.readFromWithFieldType(this.buffer, type);
    this.fields.put(i, metaField);
    return metaField;
  }

  public MetaFieldRS getMetaField(String fieldName) {
    int id = this.schema.getFieldID(fieldName);
    FieldType type = this.schema.getFieldType(fieldName);
    return (id < 0 || id >= this.fieldOffsets.length) ? null : this.getMetaField(id, type);
  }
}
