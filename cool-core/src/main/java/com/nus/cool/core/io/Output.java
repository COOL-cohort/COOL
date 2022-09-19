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

package com.nus.cool.core.io;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Base interface for all write-only data structures.
 */
public interface Output {

  /**
   * Write data to output stream and return bytes written.
   *
   * @param out stream can write data to output stream
   * @return bytes written
   * @throws IOException If an I/O error occurs
   */
  int writeTo(DataOutput out) throws IOException;
}
