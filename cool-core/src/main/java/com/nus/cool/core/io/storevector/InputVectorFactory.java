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

import com.nus.cool.core.schema.Codec;
import java.nio.ByteBuffer;

/**
 * Decompress stored data
 */
public class InputVectorFactory {

  public static InputVector readFrom(ByteBuffer buffer) {
    Codec codec = Codec.fromInteger(buffer.get());
    InputVector result = null;
    switch (codec) {
      case INT8:
        return (InputVector) ZInt8Store.load(buffer, buffer.getInt());
      case INT16:
        return (InputVector) ZInt16Store.load(buffer, buffer.getInt());
      case INT32:
        return (InputVector) ZInt32Store.load(buffer, buffer.getInt());
      case BitVector:
        result = new BitVectorInputVector();
        result.readFrom(buffer);
        return result;
      case LZ4:
        result = new LZ4InputVector();
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
        throw new IllegalArgumentException("Unsupported codec: " + codec);
    }
  }
}
