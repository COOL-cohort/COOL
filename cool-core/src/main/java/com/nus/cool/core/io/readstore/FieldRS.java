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

import com.nus.cool.core.io.Input;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.FieldType;

/**
 * Interface of read stores of fields in data chunks.
 */
public interface FieldRS extends Input {

  FieldType getFieldType();

  /**
   * Return the hash index vector. If the field is
   * indexed by range indexing, an IllegalStateException is thrown.
   *
   * @return InputVector
   *
   * @deprecated
   *             This method is no longer acceptable to get the value from FieldRS
   *             <p>
   *             Use {@link #getValueByIndex(int) instead}.
   */
  InputVector getKeyVector();

  /**
   * Return the local id of each column.
   *
   * @return InputVector
   *
   * @deprecated
   *             This method is no longer acceptable to get the value from FieldRS
   *             <p>
   *             Use {@link #getValueByIndex(int) instead}.
   *             Returns the value vector of this field.
   */
  InputVector getValueVector();

  /**
   * Returns the minKey if the field is range indexed.
   * IllegalStateException is thrown if the field is hash indexed.
   *
   * @return int
   */
  int minKey();

  /**
   * Returns the maxKey if the field is range indexed.
   * IllegalStateException is thrown if the field is hash indexed.
   *
   * @return int
   */
  int maxKey();

  /**
   * Get the global id for set, or actual value for range.
   *
   * @param idx index of tuple
   * @return int globalId of value
   */
  int getValueByIndex(int idx);

  // void readFromWithFieldType(ByteBuffer buf, FieldType fieldType);

  /**
   * Get the number of record in this field.
   *
   * @return i number of records
   */
  int getFieldSize();

}
