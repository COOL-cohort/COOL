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
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.readstore.CohortRS;
import com.nus.cool.core.io.readstore.CubletRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.RLEInputVector;
import com.nus.cool.core.schema.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

/**
 * CoolModel is a higher level abstraction of cubes for data. CoolModel can load
 * the cubes from the corresponding path.
 */
public class CoolModel implements Closeable {

    static final Logger logger = LoggerFactory.getLogger(CoolModel.class);

    // Container of loaded cubes
    private final Map<String, CubeRS> metaStore = Maps.newHashMap();

    // Container of loaded cohorts
    private final Map<String, CohortRS> cohortStore = Maps.newHashMap();

    // Store path of loaded cubes
    private final Map<String, File> storePath = Maps.newHashMap();

    // Directory containing a set of cube files considered a repository
    private final File localRepo;

    private String currentCube = "";

    public CoolCohortEngine cohortEngine = new CoolCohortEngine();

    public CoolOlapEngine olapEngine = new CoolOlapEngine();

    /**
     * Create a CoolModel to manage a cube repository.
     * It supports to create a new cube repository if it does not exist.
     *
     * @param path the repository directory
     */
    public CoolModel(String path) throws IOException {
        localRepo = new File(path);
        if (!localRepo.exists()) {
            if (localRepo.mkdir()){
                logger.info("[*] Cube repository " + localRepo.getCanonicalPath() + " is created!");
            } else {
                logger.info("[x] Cube repository " + localRepo.getCanonicalPath() + " already exist!");
            }
            // throw new FileNotFoundException("[x] Cube Repository " + localRepo.getAbsolutePath() + " was not found");
        }
    }

    /**
     * Load a cube (a set of cube files) from the repository folder into memory
     *
     * @param cube the cube name
     * @throws IOException
     */
    public synchronized void reload(String cube) throws IOException {
        // Skip the reload process if the cube is the current one
        if (currentCube.equals(cube)) return;
        // Skip the reload process if the cube is loaded
        if (isCubeLoaded(cube)) {
            if (!Objects.equals(currentCube, cube)) this.cohortStore.clear();
            currentCube = cube;
            resetCube(cube);
            return;
        }

        // Remove the old version of the cube
        this.metaStore.remove(cube);
        this.cohortStore.clear();
        this.currentCube = cube;

        // Check the existence of cube under this repository
        File cubeRoot = new File(this.localRepo, cube);
        if (!cubeRoot.exists()) {
            throw new FileNotFoundException("[x] Cube " + cube + " was not found in the repository.");
        }

        File[] versions = cubeRoot.listFiles(File::isDirectory);
        checkNotNull(versions);
        if (versions.length == 0) {
            return;
        }
        Arrays.sort(versions);

        // Only load the latest version
        File currentVersion = versions[versions.length - 1];

        // Read schema information
        TableSchema schema = TableSchema.read(new FileInputStream(new File(currentVersion, "table.yaml")));
        CubeRS cubeRS = new CubeRS(schema);

        File[] cubletFiles = currentVersion.listFiles((file, s) -> s.endsWith(".dz"));
        logger.info("Cube " + cube + ", Use version: " + currentVersion.getName());
        storePath.put(cube, currentVersion);

        // Load all cubes under latest version
        checkNotNull(cubletFiles);
        for (File cubletFile : cubletFiles) {
            cubeRS.addCublet(cubletFile);
        }

        this.metaStore.put(cube, cubeRS);
    }

    /**
     * Load from Bytebuffer.
     *
     * @param cube        cube name, data source name
     * @param buffer      input buffer
     * @param tableSchema input schema
     * @throws IOException IOException
     */
    public synchronized void reload(String cube, ByteBuffer buffer, TableSchema tableSchema) throws IOException {

        // Remove the old version of the cube
        this.metaStore.remove(cube);
        this.cohortStore.clear();
        this.currentCube = cube;

        // 1. Read schema information
        CubeRS cubeRS = new CubeRS(tableSchema);
        // 2. Load cube from buffer
        cubeRS.addCublet(buffer);
        this.metaStore.put(cube, cubeRS);
        System.out.println("Cube " + cube + ", metaStore: " + this.metaStore.keySet());
    }

    @Override
    public void close() throws IOException {

    }

    /**
     * Retrieve a cube by name
     *
     * @throws IOException
     */
    public synchronized CubeRS getCube(String cube) throws IOException {
        CubeRS out = this.metaStore.get(cube);
        if (out == null) {
            throw new IOException("[*] Cube " + cube + " is not loaded in the COOL system. Please reload it.");
        } else
            currentCube = cube;
        return out;
    }

    public static String[] listCubes(String Path) {
        File localRepoPara = new File(Path);
        return localRepoPara.list();
    }

    public synchronized boolean isCubeLoaded(String Cube) {
        return this.metaStore.containsKey(Cube);
    }

    public synchronized String[] listCohorts(String cube) throws IOException {
        File cube_cohort = new File(getCubeStorePath(cube), "cohort");
        return cube_cohort.list();
    }

    public synchronized void loadCohorts(String inputCohorts, String cube) throws IOException {
        File cohortFile = new File(new File(getCubeStorePath(cube), "cohort"), inputCohorts);
        if (cohortFile.exists()) {
            CohortRS store = CohortRS.load(Files.map(cohortFile).order(ByteOrder.nativeOrder()));
            this.cohortStore.put(cohortFile.getName(), store);
        } else {
            throw new IOException("[x] Cohort File " + cohortFile + " does not exist in the cube " + cube + ".");
        }
    }

    public InputVector getCohortUsers(String cohort) throws IOException {
        if (cohort == null) return null;
        if (!cohortStore.containsKey(cohort)) {
            loadCohorts(cohort, this.currentCube);
        }
        InputVector ret = cohortStore.get(cohort).getUsers();
        return ret;
    }

    public File getCubeStorePath(String cube) throws IOException {
        File out = this.storePath.get(cube);
        if (out == null) {
            throw new IOException("[x] Cube " + cube + " is not loaded in the COOL system. Please reload it.");
        } else
            return out;

    }

 

    public void resetCube(String cube_name) throws IOException {
        CubeRS cube = this.metaStore.get(cube_name);
        int userKeyId = cube.getTableSchema().getUserKeyField();
        for (CubletRS cubletRS : cube.getCublets()) {
            for (ChunkRS dataChunk : cubletRS.getDataChunks()) {
                FieldRS userField = dataChunk.getField(userKeyId);
                RLEInputVector userInput = (RLEInputVector) userField.getValueVector();
                userInput.skipTo(0);
            }
        }
        System.out.println("Cube " + cube + " has been reset.");
    }

    public void clearCohorts() throws IOException {
        this.cohortStore.clear();
    }

    /**
     * Check whether cube is loaded before
     * @param cube
     * @return
     * @throws IOException
     */
    public synchronized boolean isCubeExist(String cube) throws IOException{
      File cubeRoot = new File(this.localRepo, cube);
      return cubeRoot.exists();
  
    }
}
