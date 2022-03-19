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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.readstore.CohortRS;
import com.nus.cool.core.schema.TableSchema;

import java.io.*;
import java.nio.ByteOrder;
import java.util.*;

/**
 * CoolModel is a higher level abstraction of cubes for data. CoolModel can load
 * the cubes from the corresponding path.
 */
public class CoolModel implements Closeable {

  // Container of loaded cubes
  private final Map<String, CubeRS> metaStore = Maps.newHashMap();

  // Container of loaded cohorts
  private Map<String, CohortRS> cohortStore = Maps.newHashMap();

  // Store path of loaded cubes
  private Map<String, File> storePath = Maps.newHashMap();

  // Directory containing a set of cube files considered a repository
  private final File localRepo;

  /**
   * Create a CoolModel to manage a cube repository
   *
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
    // Remove the old version of the cube
    this.metaStore.remove(cube);
    this.cohortStore.clear();

    // Check the existence of cube under this repository
    File cubeRoot = new File(this.localRepo, cube);
      if (!cubeRoot.exists()) {
        throw new FileNotFoundException("[x] Cube " + cube + " was not found");
      }

    // Read schema information
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

    // Only load the latest version
    File currentVersion = versions[versions.length - 1];
    File[] cubletFiles = currentVersion.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File file, String s) {
        return s.endsWith(".dz");
      }
    });
    System.out.println("Cube " + cube + ", versions: " + Arrays.toString(versions));
    System.out.println("Cube " + cube + ", Use version: " + currentVersion.getName());
    storePath.put(cube, currentVersion);

    // Load all cubes under latest version
    checkNotNull(cubletFiles);
      for (File cubletFile : cubletFiles) {
          cubeRS.addCublet(cubletFile);
      }
    this.metaStore.put(cube, cubeRS);
    System.out.println("Cube " + cube + ", metaStore: " + this.metaStore.keySet());
  }

  @Override
  public void close() throws IOException {

  }

  /**
   * Retrive a cube by name
   */
  public synchronized CubeRS getCube(String cube) throws IOException{
    CubeRS out = this.metaStore.get(cube);
    if(out == null){
      throw new IOException("[*] Cube " + cube + " is not found in the system. Please reload it.");
    }
    else
      return out;
  }

  public synchronized String[] listCubes() {
    return this.localRepo.list();
  }

  public synchronized String[] listCohorts(String cube) {
    File cube_cohort = new File(new File(this.localRepo,cube), "cohort");
    return cube_cohort.list();
  }

  public synchronized void loadCohorts(String inputCohorts, String dataPath) throws IOException {
    File cohortFile = new File(dataPath+"/cohort/"+inputCohorts);
    System.out.println("Cohort File: " + cohortFile + ". It exists:" + cohortFile.exists());
    CohortRS store = CohortRS.load(Files.map(cohortFile).order(ByteOrder.nativeOrder()));
    this.cohortStore.put(cohortFile.getName(), store);
  }

  public InputVector getCohortUsers(String cohort) {
    if (cohortStore.containsKey(cohort)) {
      InputVector ret = cohortStore.get(cohort).getUsers();
      return ret;
    }
    return null;
  }

  public File getCubeStorePath(String cube) throws IOException{
    File out = this.storePath.get(cube);
    if(out == null){
      throw new IOException("[*] Cube " + cube + " is not found in the system. Please reload it.");
    }
    else
      return out;

  }
}
