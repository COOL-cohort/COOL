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

package com.nus.cool.core.io.compression;

import com.nus.cool.core.field.FieldValue;
import java.nio.charset.Charset;
import lombok.Builder;
import lombok.Getter;

/**
 * Properties for compress.
 */
@Getter
@Builder
public class Histogram {
  // /**
  //  * Compress data size.
  //  */
  // private int rawSize; // maybe we can have this later.

  private Charset charset;

  /**
   * Means data value occurs in many consecutive data elements.
   */
  private boolean sorted;

  /**
   * Number of values if compress data is countable.
   */
  private int numOfValues;

  /**
   * Max(Last) value in compress data.
   */
  private FieldValue max;

  /**
   * Min(First) value in compress data.
   */
  private FieldValue min;

  // /**
  //  * Specific compress type.
  //  */
  // private CompressType type;

  // private int uniqueValues;
}
