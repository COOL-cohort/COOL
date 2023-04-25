// /*
//  * Licensed to the Apache Software Foundation (ASF) under one
//  * or more contributor license agreements.  See the NOTICE file
//  * distributed with this work for additional information
//  * regarding copyright ownership.  The ASF licenses this file
//  * to you under the Apache License, Version 2.0 (the
//  * "License"); you may not use this file except in compliance
//  * with the License.  You may obtain a copy of the License at
//  *
//  *   http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing,
//  * software distributed under the License is distributed on an
//  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  * KIND, either express or implied.  See the License for the
//  * specific language governing permissions and limitations
//  * under the License.
//  */

// package com.nus.cool.core.io.readstore;

// import com.nus.cool.core.io.compression.SimpleBitSetCompressor;
// import com.nus.cool.core.io.storevector.InputVector;
// import com.nus.cool.core.io.storevector.InputVectorFactory;
// import com.nus.cool.core.schema.Codec;
// import com.nus.cool.core.schema.FieldType;
// import java.nio.ByteBuffer;
// import java.util.BitSet;

// /**
//  * Cool field read store, both hash field and range field.
//  * <p>
//  * hash field Layout
//  * -----------------
//  * | keys | values |
//  * -----------------
//  * where keys = globalIDs
//  * (compressed) values = column data, stored as localIDs (compressed)
//  * <p>
//  * range field layout
//  * ------------------------------
//  * | codec | min | max | values |
//  * ------------------------------
//  * where
//  * min = min of the values
//  * max = max of the values
//  * values = column data (compressed)
//  */
// public class CoolFieldRS implements FieldRS {

//   private FieldType fieldType;

//   private boolean bSetField;

//   private int minKey;

//   private int maxKey;

//   /**
//    * key vector for hash field, store globalIDs.
//    */
//   private InputVector keyVec = null;

//   /**
//    * value vector for hash field.
//    */
//   private InputVector valueVec = null;

//   /**
//    * BitSet array if this field has been pre-calculated.
//    */
//   private BitSet[] bitSets = null;

//   @Override
//   public void readFrom(ByteBuffer buffer) {
//     // Get field type
//     this.fieldType = FieldType.fromInteger(buffer.get());
//     readFromWithFieldType(buffer, this.fieldType);
//   }

//   @Override
//   public FieldType getFieldType() {
//     return fieldType;
//   }

//   @Override
//   public InputVector getKeyVector() {
//     return this.keyVec;
//   }

//   @Override
//   public InputVector getValueVector() {
//     return this.valueVec;
//   }

//   @Override
//   public int minKey() {
//     return this.minKey;
//   }

//   @Override
//   public int maxKey() {
//     return this.maxKey;
//   }

//   public boolean isSetField() {
//     return this.bSetField;
//   }

//   /**
//    * IO interface.
//    *
//    * @param buffer input
//    * @param fieldType fieldtype
//    */
//   public void readFromWithFieldType(ByteBuffer buffer, FieldType fieldType) {
//     this.fieldType = fieldType;
//     int bufGet = buffer.get();
//     Codec codec = Codec.fromInteger(bufGet);
//     if (codec == Codec.Range) {
//       // Range field case
//       this.minKey = buffer.getInt();
//       this.maxKey = buffer.getInt();
//       this.bSetField = false;
//     } else {
//       // Hash field case
//       buffer.position(buffer.position() - 1);
//       this.keyVec = InputVectorFactory.readFrom(buffer);
//       this.minKey = 0;
//       this.maxKey = this.keyVec.size();
//       this.bSetField = true;
//     }

//     bufGet = buffer.get();
//     codec = Codec.fromInteger(bufGet);
//     if (codec == Codec.PreCAL) {
//       int values = buffer.get();
//       this.bitSets = new BitSet[values];
//       for (int i = 0; i < values; i++) {
//         this.bitSets[i] = SimpleBitSetCompressor.read(buffer);
//       }
//     } else {
//       buffer.position(buffer.position() - 1);
//       this.valueVec = InputVectorFactory.readFrom(buffer);
//     }
//   }

//   // ------ no used, keep compatiable with new version code
//   @Override
//   public int getValueByIndex(int idx) {
//     // TODO Auto-generated method stub
//     return 0;
//   }

// }
