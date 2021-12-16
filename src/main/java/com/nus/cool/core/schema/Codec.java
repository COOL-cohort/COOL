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
package com.nus.cool.core.schema;

/**
 * Code for decompressor
 *
 * @author zhongle
 * @version 0.1
 * @since 0.1
 */
public enum Codec {

  /**
   * Code for int8 data
   */
  INT8,

  /**
   * Code for int16 data
   */
  INT16,

  /**
   * Code for int32 data
   */
  INT32,

  /**
   * Code for bit vector
   */
  BitVector,

  /**
   * Code for string values data
   */
  LZ4,

  /**
   * Code for pre calculate data
   */
  PreCAL,

  /**
   * Code for sorted data
   */
  RLE,

  // TODO: NEED docs
  INTBit,

  /**
   * Code for range, write a min value and a max value directly.
   */
  Range,

  /**
   * Code for numeric data, use delta encoding
   */
  Delta;

  public static Codec fromInteger(int c) {
    switch (c) {
      case 0:
        return INT8;
      case 1:
        return INT16;
      case 2:
        return INT32;
      case 3:
        return BitVector;
      case 4:
        return LZ4;
      case 5:
        return PreCAL;
      case 6:
        return RLE;
      case 7:
        return INTBit;
      case 8:
        return Range;
      case 9:
        return Delta;
      default:
        throw new IllegalArgumentException("Invalid codec ordinal: " + c);

    }
  }

}
