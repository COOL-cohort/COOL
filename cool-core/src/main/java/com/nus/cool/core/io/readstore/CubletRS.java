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

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.nus.cool.core.io.Input;
import com.nus.cool.core.io.compression.SimpleBitSetCompressor;
import com.nus.cool.core.schema.TableSchema;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read cublet store
 * <p>
 * cublet layout
 * ---------------------------------------------------------------------------
 * |data chunk 1 | ... | data chunk n | meta chunk ｜ header | header offset |
 * ---------------------------------------------------------------------------
 * <p>
 * header layout
 * ----------------------------------
 * | #chunks | chunk header offsets |
 * ----------------------------------
 * where
 * #chunks = number of chunk, including meta chunk
 * chunk header offsets record the position of the chunk's header
 */
public class CubletRS implements Input {

  static final Logger logger = LoggerFactory.getLogger(CubletRS.class);

  /**
   * MetaChunk for this cublet.
   */
  @Getter
  private MetaChunkRS metaChunk;

  /**
   * BitSet list for query result.
   */
  private List<BitSet> bitSets = Lists.newArrayList();

  @Getter
  private List<ChunkRS> dataChunks = Lists.newArrayList();

  private TableSchema schema;

  @Getter
  @Setter
  private String file;

  @Getter
  private int limit;

  public CubletRS(TableSchema schema) {
    this.schema = checkNotNull(schema);
  }

  /**
   * deserialize a cublet from a byte buffer.
   */
  @Override
  public void readFrom(ByteBuffer buffer) {
    logger.debug("readFrom: buffer.limit()=" + buffer.limit());

    // Read header offset
    int end = buffer.limit();
    this.limit = end;
    int headOffset;
    buffer.position(end - Ints.BYTES); // one byte to store header offset
    int tag = buffer.getInt();
    // if offset is got from last one byte
    if (tag != 0) {
      headOffset = tag;
    } else {
      // if offset is not got from last one byte, read two bytes
      buffer.position(end - Ints.BYTES - Ints.BYTES);
      int size = buffer.getInt();
      buffer.position(end - Ints.BYTES - Ints.BYTES - Ints.BYTES);
      end = buffer.getInt();
      buffer.position(end - Ints.BYTES);
      headOffset = buffer.getInt();
      buffer.position(end);
      for (; size > 0; size--) {
        this.bitSets.add(SimpleBitSetCompressor.read(buffer));
      }
    }

    logger.debug("headOffset=" + headOffset);

    // Get #chunk and chunk offsets
    buffer.position(headOffset);
    int chunks = buffer.getInt();
    int[] chunkOffsets = new int[chunks];
    for (int i = 0; i < chunks; i++) {
      chunkOffsets[i] = buffer.getInt();
      logger.debug("chunkOffsets[" + i + "]=" + chunkOffsets[i]);
    }

    // read the metaChunk, which is the last one in #chunks
    this.metaChunk = new MetaChunkRS(this.schema);
    buffer.position(chunkOffsets[chunks - 1]);
    int chunkHeadOffset = buffer.getInt();
    buffer.position(chunkHeadOffset);
    this.metaChunk.readFrom(buffer);

    // read the dataChunk
    for (int i = 0; i < chunks - 1; i++) {
      buffer.position(chunkOffsets[i]);
      chunkHeadOffset = buffer.getInt();
      logger.debug("chunkHeadOffset[" + i + "]=" + chunkHeadOffset);
      buffer.position(chunkHeadOffset);
      ChunkRS chunk = new ChunkRS(this.schema, this.metaChunk);
      chunk.readFrom(buffer);
      this.dataChunks.add(chunk);
    }

  }
}
