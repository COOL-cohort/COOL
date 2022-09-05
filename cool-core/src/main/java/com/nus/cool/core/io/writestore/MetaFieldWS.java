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
package com.nus.cool.core.io.writestore;

import com.nus.cool.core.io.Output;
import com.nus.cool.core.schema.FieldType;

public interface MetaFieldWS extends Output {

  /**
   * Put value into this field
   *
   * @param v value
   */
  void put(String v);

  /**
   * Update the meta with value
   * @param v value
   */
  void update(String v);

  /**
   * Find the index of value in this meta field, return -1 if no such value exists
   *
   * @param v target value
   * @return index of value in this meta field
   */
  int find(String v);

  /**
   * Number of entries in this field
   *
   * @return number of entries in this field
   */
  int count();

  /**
   * Get field type of this field
   *
   * @return fieldType of this field
   */
  FieldType getFieldType();

  /**
   * Call this method before writeTo when no more values are put into this meta field. After the
   * method returns, this meta field is frozen for writing.
   */
  void complete();
}
