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

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

/**
 * structure representing a dimension (column).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Dimension {

  /**
   * Type of a dimension.
   */
  public static enum DimensionType {

    NORMAL,

    PROPERTY,

    CALC

  }

  private DimensionType type;

  private String name;

  private String tableFieldName;

  private List<String> values;

  private FieldType fieldType;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getTableFieldName() {
    return tableFieldName;
  }

  public void setTableFieldName(String tableFieldName) {
    this.tableFieldName = tableFieldName;
  }

  public DimensionType getDimensionType() {
    return type;
  }

  public void setDimensionType(DimensionType type) {
    this.type = type;
  }

  public List<String> getValues() {
    return values;
  }

  public void setValues(List<String> values) {
    this.values = values;
  }

  public FieldType getTableFieldType() {
    return fieldType;
  }

  public void setTableFieldType(FieldType fieldType) {
    this.fieldType = fieldType;
  }
}
