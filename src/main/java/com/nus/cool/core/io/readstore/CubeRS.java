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
package com.nus.cool.core.io.readstore;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.nus.cool.core.schema.TableSchema;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.List;
import lombok.Getter;

/**
 *
 * @author zhongle
 * @version 0.1
 * @since 0.1
 */
public class CubeRS {

  @Getter
  private TableSchema schema;

  private List<File> cubletFiles = Lists.newArrayList();

  @Getter
  private List<CubletRS> cublets = Lists.newArrayList();

  public CubeRS(TableSchema schema) {
    this.schema = schema;
  }

  public void addCublet(File cubleFile) throws IOException {
    this.cubletFiles.add(cubleFile);
    CubletRS cubletRS = new CubletRS(this.schema);
    cubletRS.readFrom(Files.map(cubleFile).order(ByteOrder.nativeOrder()));
    cubletRS.setFile(cubleFile.getName());
    this.cublets.add(cubletRS);
  }
}
