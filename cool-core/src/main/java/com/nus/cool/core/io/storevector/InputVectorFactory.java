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

import com.nus.cool.core.field.FloatRangeField;
import com.nus.cool.core.field.IntRangeField;
import com.nus.cool.core.field.StringHashField;
import com.nus.cool.core.schema.Codec;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Decompress stored data.
 */
public class InputVectorFactory {

  private InputVectorFactory() {}

  /**
   * Create an input vector for values from a buffer.
   * Delta and RLE are not used for now.
   * BitVector is only used for keyhash.
   */
  public static RangeFieldInputVector genRangeFieldInputVector(ByteBuffer buffer)
      throws IllegalArgumentException {
    Codec codec = Codec.fromInteger(buffer.get());
    switch (codec) {
      case INT8: {
        ZInt8Store iv = new ZInt8Store();
        iv.readFrom(buffer);
        return x -> new IntRangeField(iv.get(x));
      }
      case INT16: {
        ZInt16Store iv = new ZInt16Store();
        iv.readFrom(buffer);
        return x -> new IntRangeField(iv.get(x));
      }
      case INT32: {
        ZInt32Store iv = new ZInt32Store();
        iv.readFrom(buffer);
        return x -> new IntRangeField(iv.get(x));
      }
      case RLE: {
        RLEInputVector iv = new RLEInputVector();
        iv.readFrom(buffer);
        return x -> new IntRangeField(iv.get(x));
      }
      case INTBit: {
        ZIntStore iv = ZIntBitInputVector.load(buffer);
        iv.readFrom(buffer);
        return x -> new IntRangeField(iv.get(x));
      }
      case Delta: {
        FoRInputVector iv = new FoRInputVector();
        iv.readFrom(buffer);
        return x -> new IntRangeField(iv.get(x));      
      }
      case Float: {
        FloatInputVector iv = new FloatInputVector();
        iv.readFrom(buffer);
        return x -> new FloatRangeField(iv.get(x));      
      }
      default:
        throw new IllegalArgumentException("Invalid codec for RangeFieldInputVector: " + codec);
    }
  }

  /**
   * Build an integer input vector on the data buffer. 
   */
  public static InputVector<Integer> genIntInputVector(ByteBuffer buffer)
      throws IllegalArgumentException {
    Codec codec = Codec.fromInteger(buffer.get());
    InputVector<Integer> result;
    switch (codec) {
      case INT8:
        result = new ZInt8Store();
        result.readFrom(buffer);
        return result;
      case INT16:
        result = new ZInt16Store();
        result.readFrom(buffer);
        return result;
      case INT32:
        result = new ZInt32Store();
        result.readFrom(buffer);
        return result;
      case BitVector:
        result = new BitVectorInputVector();
        result.readFrom(buffer);
        return result;
      case RLE:
        result = new RLEInputVector();
        result.readFrom(buffer);
        return result;
      case INTBit:
        return ZIntBitInputVector.load(buffer);
      case Delta:
        result = new FoRInputVector();
        result.readFrom(buffer);
        return result;
      default:
        throw new IllegalArgumentException("Unsupported codec for integer values: " + codec);
    }
  }

  public static IntFieldInputVector genIntFieldInputVector(ByteBuffer buffer)
      throws IllegalArgumentException {
    InputVector<Integer> iv = genIntInputVector(buffer);
    return x -> new IntRangeField(iv.get(x));
  }

  // string is for now the only hash field type.
  public static HashFieldInputVector genHashFieldInputVector(ByteBuffer buffer, Charset charset)
      throws IllegalArgumentException {
    InputVector<String> iv = genStrFieldInputVector(buffer, charset);
    return x -> new StringHashField(iv.get(x));
  }

  /**
   * Build an string input vector on the data buffer.
   */
  public static InputVector<String> genStrFieldInputVector(ByteBuffer buffer, Charset charset)
      throws IllegalArgumentException {
    Codec codec = Codec.fromInteger(buffer.get());
    // only lz4 encoding for string field now.
    if (codec != Codec.LZ4) {
      throw new IllegalArgumentException("Unsupported codec for string values: " + codec);
    }
    InputVector<String> iv = new LZ4InputVector(charset);
    iv.readFrom(buffer);
    return iv;
  }
}
