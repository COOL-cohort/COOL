/*
 * Copyright 2021 Cool Squad Team
 *
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Maps;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

/**
 * TableSchema defines the schema for data table
 */
public class TableSchema {

  @Getter
  @Setter
  private String charset;
  
  /**
   * fields is a set of field.
   */
  @Getter
  private List<FieldSchema> fields;
  
  /**
   * name2Id is the mapping of field name and the corresponding id
   */
  private Map<String, Integer> name2Id = Maps.newHashMap();

  /**
   * UserKeyField index, assign -1 if this field type not exist.
   */
  @Getter
  private int userKeyField = -1;

  /**
   * AppKeyField index, assign -1 if this field type not exist.
   */
  @Getter
  private int appKeyField = -1;

  /**
   * ActionField index, assign -1 if this field type not exist.
   */
  @Getter
  private int actionField = -1;

  /**
   * ActionTimeField index, assign -1 if this field type not exist.
   */
  @Getter
  private int actionTimeField = -1;

  /**
   * Obtain the content of a table from input stream
   *
   * @param in the stream containing the content of the table
   * @return the content of the table
   */
  public static TableSchema read(InputStream in) throws IOException {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    return mapper.readValue(in, TableSchema.class);
  }

  /**
   * Obtain the content of a table from a file
   *
   * @param inputFile the File object of the file to be read
   * @return the content of the table
   */
  public static TableSchema read(File inputFile) throws IOException {
    return read(new FileInputStream(inputFile));
  }
  
  // Update the fields according to the input data
  public void setFields(List<FieldSchema> fields) {
    this.fields = fields;
    this.name2Id.clear();
    for (int i = 0; i < fields.size(); i++) {
      FieldSchema field = fields.get(i);
      FieldType fieldType = field.getFieldType();
      this.name2Id.put(field.getName(), i);
      switch (fieldType) {
        case AppKey:
          this.appKeyField = i;
          break;
        case UserKey:
          this.userKeyField = i;
          break;
        case Action:
          this.actionField = i;
          break;
        case ActionTime:
          this.actionTimeField = i;
          break;
        default:
          break;
      }
    }
  }


  /**
   * Get the field by it's id
   *
   * @param id the id of the field
   * @return the corresponding field of the id
   */
  public FieldSchema getField(int id) {
    return this.fields.get(id);
  }

  /**
   * Get the field by it's name
   *
   * @param name the name of the field
   * @return the corresponding field of the name
   */
  public FieldSchema getField(String name) {
    return this.getField(this.getFieldID(name));
  }

  /**
   * Get the field type by it's name
   *
   * @param name the name of the field
   * @return the corresponding field type of the name
   */
  public FieldType getFieldType(String name) {
    return this.getField(name).getFieldType();
  }

  /**
   * Get the field id by it's name
   *
   * @param name the name of the field
   * @return the corresponding field id of the name
   */
  public int getFieldID(String name) {
    Integer id = this.name2Id.get(name);
    return id == null ? -1 : id;
  }

  /**
   * Get the field which denotes the action time
   *
   * @return the corresponding action time field
   */
  public String getActionTimeFieldName() {
    return this.fields.get(this.getActionTimeField()).getName();
  }
}
