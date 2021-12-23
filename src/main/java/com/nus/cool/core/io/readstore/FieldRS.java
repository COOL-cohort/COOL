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

import com.nus.cool.core.io.Input;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.FieldType;
import java.nio.ByteBuffer;

/**
 *
 * @author zhongle
 * @version 0.1
 * @since 0.1
 */
public interface FieldRS extends Input {

  /**
   * Return the hash index vector. If the field is
   * indexed by range indexing, an IllegalStateException
   * is thrown.
   *
   * @return
   */

  InputVector getKeyVector();

  /**
   * Returns the value vector of this field.
   *
   * @return
   */
  InputVector getValueVector();

  /**
   * Returns the minKey if the field is range indexed.
   * IllegalStateException is thrown if the field is hash indexed.
   *
   * @return
   */
  int minKey();

  /**
   * Returns the maxKey if the field is range indexed.
   * IllegalStateException is thrown if the field is hash indexed.
   *
   * @return
   */
  int maxKey();

  /**
   * Returns true if the field is a hash(i.e., set) indexed field
   * @return
   */
  boolean isSetField();

  void readFromWithFieldType(ByteBuffer buf, FieldType fieldType);
}
