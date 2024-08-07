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

package com.nus.cool.model;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.nus.cool.core.io.readstore.CohortRS;
import com.nus.cool.core.io.readstore.CubeMetaRS;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.TableSchema;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CoolModel is a higher level abstraction of cubes for data. CoolModel can load
 * the cubes from the corresponding path.
 */
public class CoolModel implements Closeable {

  static final Logger logger = LoggerFactory.getLogger(CoolModel.class);

  // Container of loaded cubes, e,g. currentCube: CubeRS
  private final Map<String, CubeRS> cubeStore = Maps.newHashMap();

  // separate store for cube meta (not needed for processing)
  private final Map<String, CubeMetaRS> cubeMetaStore = Maps.newHashMap();

  // Container of loaded cohorts
  private final Map<String, CohortRS> cohortStore = Maps.newHashMap();

  // Store file paths of currentCube (contains version information),
  // e,g. File(./yaml, /data.dz, ./metacube)
  private final Map<String, File> storePath = Maps.newHashMap();

  // Directory repository containing a set of cubes
  private final File localRepo;

  // name of the current cube, e,g. health_raw
  private String currentCube = "";

  /**
   * Create a CoolModel to manage a cube repository.
   * It supports to create a new cube repository if it does not exist.
   *
   * @param path the repository directory
   */
  public CoolModel(String path) throws IOException {
    localRepo = new File(path);
    if (!localRepo.exists()) {
      if (localRepo.mkdir()) {
        logger.info("[*] Cube repository " + localRepo.getCanonicalPath() + " is created!");
      } else {
        logger.info("[x] Cube repository " + localRepo.getCanonicalPath() + " already exist!");
      }
      // throw new FileNotFoundException("[x] Cube Repository " +
      // localRepo.getAbsolutePath() + " was not found");
    }
  }

  /**
   * Load the latest version directory of a cube.
   * caller: reload(String) and getCubeMeta(String)
   */
  public File getLatestVersion(String cube) throws IOException {
    // Check the existence of cube under this repository
    File cubeRoot = new File(this.localRepo, cube);
    if (!cubeRoot.exists()) {
      throw new FileNotFoundException("[x] Cube " + cube + " was not found in the repository.");
    }

    File[] versions = cubeRoot.listFiles(File::isDirectory);
    checkNotNull(versions);
    if (versions.length == 0) {
      throw new IOException("[x] No version found for the cube" + cube + ".");
    }
    Arrays.sort(versions);

    // Only load the latest version
    return versions[versions.length - 1];
  }

  /**
   * Load a cube (a set of cube files) from the repository folder into memory.
   *
   * @param cube the cube name
   */
  public synchronized void reload(String cube) throws IOException {
    System.out.println("reloading...");
    // Skip the reload process if the cube is the current one and is the latest one
    if (currentCube.equals(cube) && islatestCubeLoaded(cube)) {
      return;
    }
    // Skip the reload process if the cube is loaded
    //    if (islatestCubeLoaded(cube)) {
    //      if (!Objects.equals(currentCube, cube)) {
    //        this.cohortStore.clear();
    //      }
    //      currentCube = cube;
    //      resetCube(cube);
    //      return;
    //    }

    // Remove the old version of the cube
    this.cubeStore.remove(cube);
    this.cohortStore.clear();
    this.currentCube = cube;

    // Only load the latest version
    File currentVersion = getLatestVersion(cube);

    // Read schema information
    TableSchema schema = TableSchema.read(
        new FileInputStream(new File(currentVersion, "table.yaml")));
    CubeRS cubeRS = new CubeRS(schema);

    File[] cubletFiles = currentVersion.listFiles((file, s) -> s.endsWith(".dz"));
    Arrays.sort(cubletFiles);

    logger.info("Cube " + cube + ", Use version: " + currentVersion.getName());
    storePath.put(cube, currentVersion);

    logger.debug("cublet files: ");
    for (int i = 0; i < cubletFiles.length; i++) {
      logger.debug(cubletFiles[i].getPath());
    }

    // Load all cubes under latest version
    checkNotNull(cubletFiles);
    for (File cubletFile : cubletFiles) {
      logger.debug("loading cublet file: " + cubletFile);
      cubeRS.addCublet(cubletFile);
    }

    this.cubeStore.put(cube, cubeRS);
  }

  /**
   * Load from Bytebuffer.
   *
   * @param cube        cube name, data source name
   * @param buffer      input buffer
   * @param tableSchema input schema
   * @throws IOException IOException
   */
  public synchronized void reload(String cube, ByteBuffer buffer, TableSchema tableSchema)
      throws IOException {
    // Remove the old version of the cube
    this.cubeStore.remove(cube);
    this.cohortStore.clear();
    this.currentCube = cube;

    // 1. Read schema information
    CubeRS cubeRS = new CubeRS(tableSchema);
    // 2. Load cube from buffer
    cubeRS.addCublet(buffer);
    this.cubeStore.put(cube, cubeRS);
    System.out.println("Cube " + cube + ", cubeStore: " + this.cubeStore.keySet());
  }

  @Override
  public void close() throws IOException {

  }

  /**
   * Retrieve a cube by name.
   */
  public synchronized CubeRS getCube(String cube) throws IOException {
    CubeRS out = this.cubeStore.get(cube);
    if (out == null) {
      throw new IOException("[*] Cube " + cube
          + " is not loaded in the COOL system. Please reload it.");
    } else {
      currentCube = cube;
    }
    return out;
  }

  /**
   * Get CubeMeta of a cube.
   */
  public synchronized CubeMetaRS getCubeMeta(String cube) throws IOException {
    CubeMetaRS cubeMeta = this.cubeMetaStore.get(cube);
    if (cubeMeta != null) {
      return cubeMeta;
    }

    // load from the latest version
    File currentVersion = getLatestVersion(cube);

    // Read schema information
    TableSchema schema = TableSchema.read(
        new FileInputStream(new File(currentVersion, "table.yaml")));

    // load the cube meta.
    cubeMeta = new CubeMetaRS(schema);
    File cubeMetaFile = new File(currentVersion, "cubemeta");
    cubeMeta.readFrom(Files.map(cubeMetaFile));
    this.cubeMetaStore.put(cube, cubeMeta);
    return cubeMeta;
  }

  public static String[] listCubes(String path) {
    File localRepoPara = new File(path);
    return localRepoPara.list();
  }

  /**
   * check whether the cube ls loaded and is the latest.
   *
   * @param cube the name of the cube
   * @return whether the cube ls loaded and is the latest
   * @throws IOException error
   */
  public synchronized boolean islatestCubeLoaded(String cube) throws IOException {
    // loaded and latest
    return this.cubeStore.containsKey(cube) && this.storePath.get(cube).getName().equals(
        getLatestVersion(cube).getName());
  }

  public synchronized String[] listCohorts(String cube) throws IOException {
    File cubeCohort = new File(getCubeStorePath(cube), "cohort");
    return cubeCohort.list();
  }

  /**
   * Load a previously generated cohort of a cube.
   */
  public synchronized void loadCohorts(String inputCohorts, String cube) throws IOException {
    File cohortFile = new File(new File(getCubeStorePath(cube), "cohort"), inputCohorts);
    if (cohortFile.exists()) {
      CohortRS store = CohortRS.load(Files.map(cohortFile));
      this.cohortStore.put(cohortFile.getName(), store);
    } else {
      throw new IOException("[x] Cohort File " + cohortFile
          + " does not exist in the cube " + cube + ".");
    }
  }

  /**
   * Get users of a previously generated cohort.
   */
  public InputVector<Integer> getCohortUsers(String cohort) throws IOException {
    if (cohort == null) {
      return null;
    }
    if (!cohortStore.containsKey(cohort)) {
      loadCohorts(cohort, this.currentCube);
    }
    return cohortStore.get(cohort).getUsers();
  }

  /**
   * Construct the cube file path.
   */
  public File getCubeStorePath(String cube) throws IOException {
    File out = this.storePath.get(cube);
    if (out == null) {
      throw new IOException("[x] Cube " + cube
          + " is not loaded in the COOL system. Please reload it.");
    } else {
      return out;
    }
  }

  /**
   * Reset a cube. [TODO] should rethink the logic
   */
  public void resetCube(String cubeName) throws IOException {
    // CubeRS cube = 
    this.cubeStore.get(cubeName);
    // [TODO] implement checking and loading of cublet cache.
    
    // int userKeyId = cube.getTableSchema().getUserKeyFieldIdx();
    // for (CubletRS cubletRS : cube.getCublets()) {
    //   for (ChunkRS dataChunk : cubletRS.getDataChunks()) {
    //     FieldRS userField = dataChunk.getField(userKeyId);
    //     RLEInputVector userInput = (RLEInputVector) userField.getValueVector();
    //     userInput.skipTo(0);
    //   }
    // }
    // System.out.println("Cube " + cube + " has been reset.");
  }

  public void clearCohorts() throws IOException {
    this.cohortStore.clear();
  }

  /**
   * Check whether cube is loaded before.
   */
  public synchronized boolean isCubeExist(String cube) throws IOException {
    File cubeRoot = new File(this.localRepo, cube);
    return cubeRoot.exists();
  }

  /**
   * Load the latest version directory of a cube.
   * caller: reload(String) and getCubeMeta(String)
   */
  public String[] getAllVersions(String cube) throws IOException {
    // Check the existence of cube under this repository
    File cubeRoot = new File(this.localRepo, cube);
    if (!cubeRoot.exists()) {
      throw new FileNotFoundException("[x] Cube " + cube + " was not found in the repository.");
    }

    File[] versions = cubeRoot.listFiles(File::isDirectory);
    assert versions != null;
    return Arrays.stream(versions)
        .map(File::getName)
        .toArray(String[]::new);
  }

  /**
   * getCubeColumns.
   */
  public String[] getCubeColumns(String cube) {
    CubeRS cubeRS = this.cubeStore.get(cube);
    return cubeRS
        .getTableSchema()
        .getFields()
        .stream()
        .map(FieldSchema::toString)
        .toArray(String[]::new);
  }
}
