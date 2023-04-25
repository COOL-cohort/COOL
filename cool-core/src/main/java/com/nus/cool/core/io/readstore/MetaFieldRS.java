/*
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

import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.RangeField;
import com.nus.cool.core.io.Input;
import com.nus.cool.core.schema.FieldType;
import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * For the moment, we only support Hash based meta field.
 */
public interface MetaFieldRS extends Input {

  FieldType getFieldType();

  /**
   * Return the id of the given value.
   */
  int find(String key);

  /**
   * Return number of values in the field.
   */
  int count();

  /**
   * Return the value for the given id.
   */
  Optional<? extends FieldValue> get(int i);

  /**
   * Return the max value of the field.
   */
  RangeField getMaxValue();

  /**
   * Return the min value of the field.
   */
  RangeField getMinValue();

  void readFromWithFieldType(ByteBuffer buffer, FieldType fieldType)
      throws IllegalArgumentException;
}
