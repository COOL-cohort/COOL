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

package com.nus.cool.core.io.readstore;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.nus.cool.core.schema.TableSchema;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import lombok.Getter;

/**
 * The in-memory data structure for cube.
 */
public class CubeRS {

  /**
   * Schema information of this cube.
   */
  @Getter
  private final TableSchema schema;

  /**
   * Loaded cublets.
   */
  @Getter
  private final List<CubletRS> cublets = Lists.newArrayList();

  public CubeRS(TableSchema schema) {
    this.schema = schema;
  }

  /**
   * Load a cubelet from a file.
   *
   * @param cubletFile read from cubletFile
   */
  public void addCublet(File cubletFile) throws IOException {
    CubletRS cubletRS = new CubletRS(this.schema);
    cubletRS.readFrom(Files.map(cubletFile));
    cubletRS.setFile(cubletFile.getName());
    this.cublets.add(cubletRS);
  }

  /**
   * Load from ByteByffer.
   *
   * @param buffer read from byteBuffer
   */
  public void addCublet(ByteBuffer buffer) {
    CubletRS cubletRS = new CubletRS(this.schema);
    cubletRS.readFrom(buffer);
    this.cublets.add(cubletRS);

  }

  public TableSchema getTableSchema() {
    return this.schema;
  }
}
