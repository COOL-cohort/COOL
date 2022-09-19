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

package com.nus.cool.core.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * Scheme of a cube.
 */
public class CubeSchema {

  private List<Dimension> dimensions;

  private List<Measure> measures;

  @JsonIgnore
  private final Map<String, Integer> dimenMap = Maps.newHashMap();

  @JsonIgnore
  private final Map<String, Integer> measureMap = Maps.newHashMap();

  public static CubeSchema read(InputStream in) throws IOException {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    return mapper.readValue(in, CubeSchema.class);
  }

  public static CubeSchema read(File schemaFile) throws IOException {
    return read(new FileInputStream(schemaFile));
  }

  /**
   * Return the dimensions.
   */
  public List<Dimension> getDimensions() {
    return dimensions;
  }

  /**
   * Get a dimension by its name.
   */
  public Dimension getDimension(String name) {
    if (dimenMap.containsKey(name)) {
      return dimensions.get(dimenMap.get(name));
    }
    return null;
  }

  /**
   * Set the set of dimensions.
   */
  public void setDimensions(List<Dimension> dimensions) {
    this.dimensions = dimensions;
    int i = 0;
    for (Dimension dim : dimensions) {
      dimenMap.put(dim.getName(), i++);
    }
  }

  public List<Measure> getMeasures() {
    return measures;
  }

  /**
   * Get a measure by name.
   */
  public Measure getMeasure(String measureName) {
    if (measureMap.containsKey(measureName)) {
      return measures.get(measureMap.get(measureName));
    }
    return null;
  }

  /**
   * Set measures.
   */
  public void setMeasures(List<Measure> measures) {
    this.measures = measures;
    int i = 0;
    for (Measure measure : measures) {
      measureMap.put(measure.getName(), i++);
    }
  }

  @Override
  public String toString() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    try {
      return mapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
    return null;
  }
}
