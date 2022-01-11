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
import com.nus.cool.core.schema.FieldType;
import java.nio.ByteBuffer;

/**
 * For the moment, we only support Hash based meta field.
 * TODO: add range meta field later
 * @author zhongle
 * @version 0.1
 * @since 0.1
 */
public interface MetaFieldRS extends Input {

  FieldType getFieldType();

  /**
   * Return the id of the given value
   *
   * @param v
   * @return
   */

  int find(String key);

  /**
   * Return number of values in the field
   */
  int count();

  /**
   * Return the value for the given id
   *
   * @param i
   * @return
   */
  String getString(int i);

  /**
   * Return the max value of the field
   */
  int getMaxValue();

  /**
   * Return the min value of the field
   */
  int getMinValue();

  void readFromWithFieldType(ByteBuffer buffer, FieldType fieldType);
}
