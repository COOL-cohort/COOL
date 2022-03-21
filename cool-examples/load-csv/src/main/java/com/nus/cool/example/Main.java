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
package com.nus.cool.example;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;

import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.config.CsvDataLoaderConfig;
import com.nus.cool.core.util.config.DataLoaderConfig;
import com.nus.cool.loader.DataLoader;

public class Main {
  /**
   * 
   * @param args there are five arguments. List in input order
   *  (1) output cube name: to be specified when loading from the repository
   *  (2) table.yaml (3) dimension.csv (4) data.csv (5) output cube repository 
   * @throws IOException
   */
  public static void main(String[] args) {
    String cube = args[0];
    String schemaFileName = args[1];
    File schemaFile = new File(schemaFileName);
    File dimensionFile = new File(args[2]);
    File dataFile = new File(args[3]);
    String cubeRepo = args[4];
    try {
      TableSchema schema = TableSchema.read( new FileInputStream(schemaFile));
      File cubeRoot = new File(cubeRepo, cube);
      File[] versions = cubeRoot.listFiles(new FileFilter() {
        @Override
        public boolean accept(File file) {
          return file.isDirectory();
        }
      });
      int currentVersion = 0;
      assert versions != null;
      if (versions.length!=0){
        Arrays.sort(versions);
        File LastVersion = versions[versions.length - 1];
        currentVersion = Integer.parseInt(LastVersion.getName().substring(1));
      }

      Path outputCubeVersionDir = Paths.get(cubeRepo, cube, "v"+String.valueOf(currentVersion+1));
      Files.createDirectories(outputCubeVersionDir);
      File outputDir = outputCubeVersionDir.toFile();
      DataLoaderConfig config = new CsvDataLoaderConfig();
      DataLoader loader = DataLoader.builder(cube, schema,
        dimensionFile, dataFile, outputDir, config).build();
      loader.load();
      Files.copy(Paths.get(schemaFileName), 
        Paths.get(cubeRepo, cube, "table.yaml"), 
        StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      System.out.println("Failed to load data");
      return;
    }
    System.out.println("Data loaded");
  }
}  
