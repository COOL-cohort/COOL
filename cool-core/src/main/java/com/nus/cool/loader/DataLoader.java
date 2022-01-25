/*
 * Copyright 2021 Cool Squad Team
 *
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
package com.nus.cool.loader;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.nus.cool.core.io.writestore.ChunkWS;
import com.nus.cool.core.io.writestore.MetaChunkWS;
import com.nus.cool.core.io.writestore.MetaFieldWS;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.IntegerUtil;
import com.nus.cool.core.util.config.DataLoaderConfig;
import com.nus.cool.core.util.parser.CsvTupleParser;
import com.nus.cool.core.util.parser.TupleParser;
import com.nus.cool.core.util.reader.LineTupleReader;
import com.nus.cool.core.util.reader.TupleReader;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Cublet Data Layout
 * -------------------------------------------------------------------------
 * | metaChunk | chunk 1 | chunk 2 |....| chunk N | header | header offset |
 * -------------------------------------------------------------------------
 * 
 * Header layout: 
 * --------------------------------- 
 * | chunks | chunk header offsets | 
 * --------------------------------- 
 * where chunks == number of chunks stored in this cublet.
 * 
 * Each cubelet has a size of roughly 1G in order to be memory mapped
 */
@RequiredArgsConstructor
public class DataLoader {
  /**
   * metaChunk offset
   */
  private int offset = 0;
  /**
   * Header offsets
   */
  private List<Integer> chunkOffsets = Lists.newArrayList();

  @NonNull
  private String dataSetName;

  @NonNull
  private TableSchema tableSchema;

  @NonNull
  private final File outputDir;

  @NonNull
  private final TupleReader reader;

  @NonNull
  private final TupleParser parser;

  @NonNull
  private final MetaFieldWS[] metaFields;

  private final long chunkSize;

  private final long cubletSize;

  public static Builder builder(String dataSetName,
    TableSchema tableSchema, File dimensionFile, File dataFile,
    File outputDir, DataLoaderConfig config) {
    return new Builder(dataSetName, tableSchema, dimensionFile,
      dataFile, outputDir, config);
  }

  /**
   * Create a new cublet
   * @return output stream to write data into new cublet
   * @throws IOException
   */
  private DataOutputStream newCublet() throws IOException  {
    File cublet = new File(outputDir,
      dataSetName + Long.toHexString(System.currentTimeMillis())
      + ".dz");
    DataOutputStream out = new DataOutputStream(
      new FileOutputStream(cublet));
    offset = new MetaChunkWS(tableSchema, 0, metaFields)
      .writeTo(out);
    chunkOffsets.clear();
    chunkOffsets.add(offset - Ints.BYTES);
    return out;
  }
  /**
   * Close current cublet. Write chunk header offsets and header offset 
   *  into the cublet
   * @param out The output stream tied to the current cublet
   * @throws IOException
   */
  private void closeCublet(DataOutputStream out) throws IOException {
    int headOffset = offset;
    out.writeInt(IntegerUtil.toNativeByteOrder(chunkOffsets.size()));
    for(int chunkOff : chunkOffsets) {
      out.writeInt(IntegerUtil.toNativeByteOrder(chunkOff));
    }
    out.writeInt(IntegerUtil.toNativeByteOrder(headOffset));
    out.flush();
    out.close();
  }
  /**
   * Change to a new chunk
   * @param out The output stream tied to the current cublet
   * @param chunk The current chunk to finalize
   * @return a new chunk to write data
   * @throws IOException
   */
  private ChunkWS switchChunk(DataOutputStream out, ChunkWS chunk)
    throws IOException {
    offset += chunk.writeTo(out);
    chunkOffsets.add(offset - Ints.BYTES);
    if (offset >= cubletSize) {
      closeCublet(out);
      out = newCublet();
    }
    return ChunkWS.newChunk(tableSchema, metaFields, offset);
  }

  /**
   * load data into cool native format
   * @throws IOException
   */
  public void load() throws IOException {
    DataOutputStream out = newCublet();
    int userKeyIndex = tableSchema.getUserKeyField();
    String lastUser = null;
    int tuples = 0;
    ChunkWS chunk = ChunkWS.newChunk(tableSchema, metaFields, offset);
    while (reader.hasNext()) {
      String line = (String) reader.next();
      String[] tuple = parser.parse(line);
      String curUser = tuple[userKeyIndex];
      if (lastUser == null) {
          lastUser = curUser;
      }
      if ((!curUser.equals(lastUser)) && (tuples >= chunkSize)) {
          chunk = switchChunk(out, chunk);
          tuples = 0;
      }
      lastUser = curUser;
      chunk.put(tuple);
      tuples++;
    }
    offset += chunk.writeTo(out);
    chunkOffsets.add(offset-Ints.BYTES);
    closeCublet(out);
  }

  @AllArgsConstructor
  public static class Builder {
    /**
     * designate the dataset name in cube repository
     */
    @NonNull
    private String dataSetName;
    /**
     * table schema of the dataset
     */
    @NonNull
    private final TableSchema tableSchema;
    /**
     * dimension file of the dataset
     */
    @NonNull
    private final File dimensionFile;
    /**
     * raw data
     */
    @NonNull
    private final File dataFile;
    /**
     * output directory to store the dataset
     */
    @NonNull
    private final File outputDir;
    /**
     * configuration determines how the raw data is processed
     */
    @NonNull
    private final DataLoaderConfig config;

    private MetaFieldWS[] getMetaFields(File inputMetaFile,
      TableSchema schema)
      throws IOException {
      TupleParser parser = new CsvTupleParser();
      MetaChunkWS metaChunk = MetaChunkWS.newMetaChunkWS(schema, 0);
      try (TupleReader reader = new LineTupleReader(inputMetaFile)) {
        while (reader.hasNext()) {
          metaChunk.put(parser.parse(reader.next()));
        }
      }
      metaChunk.complete();
      return metaChunk.getMetaFields();
    }

    /**
     * 
     * @return
     * @throws IOException
     */
    public DataLoader build() throws IOException {
      return new DataLoader(dataSetName, tableSchema, outputDir,
        config.createTupleReader(dataFile),
        config.createTupleParser(tableSchema),
        getMetaFields(dimensionFile, tableSchema), config.getChunkSize(), config.getCubletSize());
    }
  }
}
