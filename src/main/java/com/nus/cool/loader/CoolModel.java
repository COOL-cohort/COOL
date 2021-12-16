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
package com.nus.cool.loader;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Maps;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.schema.TableSchema;
import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 *
 * @author hongbin, zhongle
 * @version 0.1
 * @since 0.1
 */
public class CoolModel implements Closeable {

  /**
   * container of loaded cubes
   */
  private Map<String, CubeRS> metaStore = Maps.newHashMap();

  /**
   * directory containing a set of cube files considered a repository
   */
  private File localRepo;

  /**
   * Create a CoolModel to manage a cube repository
   * @param path the repository directory
   */
  public CoolModel(String path) {
    this.localRepo = new File(path);
  }

  /**
   * Load a cube (a set of cube files) from the repository into memory
   * 
   * @param cube the cube name
   * @throws IOException
   */
  public synchronized void reload(String cube) throws IOException {
    // remove the old version of the cube
    this.metaStore.remove(cube);
    
    // check the existence of cube under this repository
    File cubeRoot = new File(this.localRepo, cube);
      if (!cubeRoot.exists()) {
          throw new FileNotFoundException(cube + " was not found");
      }
    
    // read schema information
    TableSchema schema = TableSchema.read(new FileInputStream(new File(cubeRoot, "table.yaml")));
    CubeRS cubeRS = new CubeRS(schema);
    File[] versions = cubeRoot.listFiles(new FileFilter() {
      @Override
      public boolean accept(File file) {
        return file.isDirectory();
      }
    });
    checkNotNull(versions);
      if (versions.length == 0) {
          return;
      }
    Arrays.sort(versions);

    // only load the latest version
    File currentVersion = versions[versions.length - 1];
    File[] cubletFiles = currentVersion.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File file, String s) {
        return s.endsWith(".dz");
      }
    });
    // load all cubes under latest version
    checkNotNull(cubletFiles);
      for (File cubletFile : cubletFiles) {
          cubeRS.addCublet(cubletFile);
      }
    this.metaStore.put(cube, cubeRS);
  }

  @Override
  public void close() throws IOException {

  }

  /**
   * Retrive a cube by name
   */
  public synchronized CubeRS getCube(String cube) {
    return this.metaStore.get(cube);
  }
}
