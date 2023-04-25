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

package com.nus.cool.loader;

import com.nus.cool.core.field.ValueConverterConfig;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.config.DataLoaderConfig;
import com.nus.cool.core.util.parser.TupleParser;
import com.nus.cool.core.util.reader.TupleReader;
import com.nus.cool.core.util.writer.DataWriter;
import com.nus.cool.core.util.writer.NativeDataWriter;
import java.io.File;
import java.io.IOException;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Cublet Data Layout
 * -------------------------------------------------------------------------
 * | metaChunk | chunk 1 | chunk 2 |....| chunk N | header | header offset |
 * -------------------------------------------------------------------------
 * <p>
 * Header layout:
 * ---------------------------------
 * | chunks | chunk header offsets |
 * ---------------------------------
 * where chunks == number of chunks stored in this cublet.
 * <p>
 * Each cubelet has a size of roughly 1G in order to be memory mapped
 */
@RequiredArgsConstructor
public class DataLoader {
  @NonNull
  private String dataSetName;

  @NonNull
  private final TupleReader reader;

  @NonNull
  private final TupleParser parser;

  @NonNull
  private final DataWriter writer;

  public static Builder builder(String dataSourceName, TableSchema tableSchema, File dataFile,
                                File outputDir, DataLoaderConfig config) {
    return new Builder(dataSourceName, tableSchema, dataFile, outputDir, config);
  }

  /**
   * Load data into cool native format.
   */
  public void load() throws IOException {
    // write dataChunk first
    writer.initialize();
    // read the data
    while (reader.hasNext()) {
      writer.add(parser.parse(reader.next()));
    }
    writer.finish();
  }

  /**
   * check the consistency for table.yaml and data.csv
   *
   * @param schemaFields the fields read from table.yaml
   * @param fieldNames   the field names read from data.csv
   * @return true indicates the names of the fields between the two files are consistent,\
   *      and otherwise, they are different.
   */
  public static boolean checkConsistency(List<FieldSchema> schemaFields, String[] fieldNames) {
    if (schemaFields.size() != fieldNames.length) {
      return false;
    }
    for (int i = 0; i < schemaFields.size(); i++) {
      if (!schemaFields.get(i).getName().equals(fieldNames[i])) {
        return false;
      }
    }
    return true;
  }


  /**
   * Builder of DataLoader.
   */
  @AllArgsConstructor
  public static class Builder {
    /**
     * Designate the dataset name in cube repository.
     */
    @NonNull
    private String dataSetName;

    /**
     * Table schema of the dataset.
     */
    @NonNull
    private final TableSchema tableSchema;

    /**
     * Raw data.
     */
    @NonNull
    private final File dataFile;

    /**
     * Output directory to store the dataset.
     */
    @NonNull
    private final File outputDir;

    /**
     * Configuration determines how the raw data is processed.
     */
    @NonNull
    private final DataLoaderConfig config;

    /**
     * Build Data loader.
     *
     * @return DataLoader instance
     */
    public DataLoader build() throws IOException {
      return new DataLoader(dataSetName, config.createTupleReader(dataFile),
          config.createTupleParser(tableSchema, new ValueConverterConfig()),
          new NativeDataWriter(tableSchema, outputDir, config.getChunkSize(),
              config.getCubletSize()));
    }
  }
}
