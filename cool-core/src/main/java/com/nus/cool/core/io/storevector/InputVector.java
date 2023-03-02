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

package com.nus.cool.core.io.storevector;

import com.nus.cool.core.io.Input;

/**
 * An ordered collection(sequence) of integers. Implementation of this interface should at least
 * implements sequential access method(i.e., hasNext() and next()).
 * <p>
 * If random access method(i.e., find() and get()) is implemented. The find() should be completed at
 * O(log(n)) and the get() should be completed at O(1).
 */
public interface InputVector<T> extends Input {

  /**
   * Get number of values of this vector.
   *
   * @return number of values
   */
  int size();

  /**
   * Find index by key.
   *
   * @param key target value
   * @return index
   */
  T find(T key);

  /**
   * Get value by index.
   *
   * @param index index
   * @return target value
   */
  T get(int index);

  /**
   * Get vector has next or not.
   *
   * @return boolean value has next
   */
  boolean hasNext();

  /**
   * Get next value in vector.
   *
   * @return next value
   */
  T next();

  /**
   * Skip to specific position.
   *
   * @param pos target position
   */
  void skipTo(int pos);
}
