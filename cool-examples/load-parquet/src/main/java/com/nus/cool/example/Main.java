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
package com.nus.cool.example;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.config.DataLoaderConfig;
import com.nus.cool.extension.util.config.ParquetDataLoaderConfig;
import com.nus.cool.loader.DataLoader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

public class Main {

  public static void enforceEmptyDir(Configuration conf,
    org.apache.hadoop.fs.Path path) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    if (fs.exists(path)) {
      if (!fs.delete(path, true)) {
        throw new IOException("can not delete path " + path);
      }
    }
    if (!fs.mkdirs(path)) {
      throw new IOException("can not create path " + path);
    }
  }

  private static void createSampleParquetFile() throws IOException {
    Configuration conf = new Configuration();
    org.apache.hadoop.fs.Path root =
      new org.apache.hadoop.fs.Path("tmp/parquet-sample-data/");
    enforceEmptyDir(conf, root);
    MessageType schema = MessageTypeParser.parseMessageType(
      "message test {"
      + "required binary sessionId; "
      + "required binary playerId; "
      + "required binary role; "
      + "required int32 money; "
      + "required binary event; "
      + "required binary eventDay; "
      + "required binary platform; "
      + "required binary country; "
      + "required binary continent; "
      + "required binary city; "
      + "required int32 numSession; "
      + "required int32 sessionLength; "
      + "} ");
    GroupWriteSupport.setSchema(schema, conf);
    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    WriterVersion version = WriterVersion.PARQUET_2_0;
    org.apache.hadoop.fs.Path file =
      new org.apache.hadoop.fs.Path(root, "test.parquet");
    ParquetWriter<Group> writer = ExampleParquetWriter
      .builder(HadoopOutputFile.fromPath(file, conf))
      .withWriterVersion(version)
      .withConf(conf)
      .build();
    writer.write(f.newGroup()
      .append("sessionId", "fd1ec667-75a4-415d-a250-8fbb71be7cab")
      .append("playerId", "43e3e0d84da1056")
      .append("role", "stonegolem")
      .append("money", 1638)
      .append("event", "launch")
      .append("eventDay", "2013-05-20")
      .append("platform", "3")
      .append("country", "Australia")
      .append("continent", "OC")
      .append("city", "Sydney")
      .append("numSession", 1)
      .append("sessionLength", 0)
    );
    writer.write(f.newGroup()
      .append("sessionId", "fd1ec667-75a4-415d-a250-8fbb71be7cab")
      .append("playerId", "43e3e0d84da1056")
      .append("role", "stonegolem")
      .append("money", 1638)
      .append("event", "fight")
      .append("eventDay", "2013-05-20")
      .append("platform", "3")
      .append("country", "Australia")
      .append("continent", "OC")
      .append("city", "Sydney")
      .append("numSession", 1)
      .append("sessionLength", 0)
    );
    writer.write(f.newGroup()
      .append("sessionId", "fd1ec667-75a4-415d-a250-8fbb71be7cab")
      .append("playerId", "43e3e0d84da1056")
      .append("role", "stonegolem")
      .append("money", 1638)
      .append("event", "fight")
      .append("eventDay", "2013-05-20")
      .append("platform", "3")
      .append("country", "Australia")
      .append("continent", "OC")
      .append("city", "Sydney")
      .append("numSession", 1)
      .append("sessionLength", 1)
    );
    writer.close(); 
  }
  /**
   * Please list the sogamo dataset files, because we are generating
   *  a sample parquet file for testing according to that dataset,  
   * @param args there are five arguments. List in input order
   *  (1) output cube name: to be specified when loading from the repository
   *  (2) table.yaml (3) dimension.csv (4) data.csv (5) output cube repository 
   *  (6) chunkSize(Int) number of tuples in a chunk
   * @throws IOException
   */
  public static void main(String[] args) {
    String cube = args[0];
    String schemaFileName = args[1];
    File schemaFile = new File(schemaFileName);
    File dimensionFile = new File(args[2]);
    // we create a sample parquet with the first three record from sogamo
    try {
      createSampleParquetFile();
      // File dataFile = new File(args[3]);
      File dataFile = new File("tmp/parquet-sample-data/test.parquet");
      String cubeRepo = args[4];
      TableSchema schema = TableSchema.read(
        new FileInputStream(schemaFile));
      Path outputCubeVersionDir = Paths.get(cubeRepo, cube, "v1"); 
      Files.createDirectories(outputCubeVersionDir);
      File outputDir = outputCubeVersionDir.toFile();
      int chunkSize = Integer.parseInt(args[5]);
      DataLoaderConfig config =
      new ParquetDataLoaderConfig(chunkSize, 1<<30);
      DataLoader loader = DataLoader.builder(cube, schema,
        dimensionFile, dataFile, outputDir, config).build();
      loader.load();
      Files.copy(Paths.get(schemaFileName), 
        Paths.get(cubeRepo, cube, "table.yaml"), 
        StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      e.printStackTrace();
      System.out.println("Failed to load data");
      return;
    }
    System.out.println("Data loaded");
  }
}  
