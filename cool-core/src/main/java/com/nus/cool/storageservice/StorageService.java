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

package com.nus.cool.storageservice;

import java.nio.file.Path;

/**
 * Storage Service defines external file storage services
 * that can be connected to COOL for loading and storing data.
 * 
 * This allows offloading the responsibility of availability,
 * persistence, fault tolerance of data to the storage service.
 */
public interface StorageService {

  /**
   * Load the data in storage service with the designated id to destDir.
   *
   * @param id      : name for the cube in storage service
   * @param destDir : path to the cube to store
   */
  StorageServiceStatus loadData(String id, Path destDir);

  /**
   * Store the file(s) under destDir in storage service with the designated id.
   *
   * @param id      : name for the cube in storage service
   * @param srcDir : path to the cube to store
   */
  StorageServiceStatus storeData(String id, Path srcDir);

  /**
   * Compared to loadData, the destDir is expected to follow the
   * cube directory organization. This allows only more efficient
   * implementation. (for example only loading the new versions of a cube)
   *
   * @param cube    : name for the cube in storage service
   * @param destDir : path to the cube to store
   */
  StorageServiceStatus loadCube(String cube, Path destDir);

  /**
   * Compared to storeData, the srcDir is expected to follow the
   * cube directory organization. This allows more efficient
   * implementation customized for COOL cube.
   *
   * @param cube   : name for the cube in storage service
   * @param srcDir : path to the cube to store
   */
  StorageServiceStatus storeCube(String cube, Path srcDir);
}
