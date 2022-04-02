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

import java.io.File;
import java.io.IOException;

import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.config.DataLoaderConfig;
import com.nus.cool.core.util.parser.TupleParser;
import com.nus.cool.core.util.reader.TupleReader;
import com.nus.cool.core.util.writer.DataWriter;
import com.nus.cool.core.util.writer.NativeDataWriter;

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
    @NonNull
    private String dataSetName;

    @NonNull
    private final TupleReader reader;

    @NonNull
    private final TupleParser parser;

    @NonNull
    private final DataWriter writer;
    

    public static Builder builder(String dataSetName,
                                  TableSchema tableSchema, File dimensionFile, File dataFile,
                                  File outputDir, DataLoaderConfig config) {
        return new Builder(dataSetName, tableSchema, dimensionFile,
                dataFile, outputDir, config);
    }

    /**
     * load data into cool native format
     * @throws IOException
     */
    public void load() throws IOException {
      writer.Initialize();
      while (reader.hasNext()) {
        writer.Add(parser.parse(reader.next()));
      }
      writer.Finish();
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

        /**
         *
         * @return
         * @throws IOException
         */
        public DataLoader build() throws IOException {
            return new DataLoader(dataSetName, config.createTupleReader(dataFile), config.createTupleParser(tableSchema), new NativeDataWriter(tableSchema, outputDir, config.getChunkSize(), config.getCubletSize(), dimensionFile));
        }
    }
}
