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

import static com.google.common.base.Preconditions.checkNotNull;

import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.InputVectorFactory;
import com.nus.cool.core.io.storevector.LZ4InputVector;
import com.nus.cool.core.schema.FieldType;
import com.rabinhash.RabinHashFunction32;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 *
 * @author zhongle
 * @version 0.1
 * @since 0.1
 */
public class HashMetaFieldRS implements MetaFieldRS {

  private static final RabinHashFunction32 rhash = RabinHashFunction32.DEFAULT_HASH_FUNCTION;

  private Charset charset;

  private FieldType fieldType;

  private InputVector fingerVec;

  private InputVector valueVec;

  public HashMetaFieldRS(Charset charset) {
    this.charset = checkNotNull(charset);
  }

  @Override
  public FieldType getFieldType() {
    return this.fieldType;
  }

  @Override
  public int find(String key) {
    return this.fingerVec.find(rhash.hash(key));
  }

  @Override
  public int count() {
    return this.fingerVec.size();
  }

  @Override
  public String getString(int i) {
    return ((LZ4InputVector) this.valueVec).getString(i, this.charset);
  }

  @Override
  public int getMaxValue() {
    return this.count() - 1;
  }

  @Override
  public int getMinValue() {
    return 0;
  }

  @Override
  public void readFromWithFieldType(ByteBuffer buffer, FieldType fieldType) {
    this.fieldType = fieldType;
    this.fingerVec = InputVectorFactory.readFrom(buffer);
      if (this.fieldType == FieldType.Action || this.fieldType == FieldType.Segment
          || this.fieldType == FieldType.UserKey) {
          this.valueVec = InputVectorFactory.readFrom(buffer);
      }
  }

  @Override
  public void readFrom(ByteBuffer buffer) {
    FieldType fieldType = FieldType.fromInteger(buffer.get());
    this.readFromWithFieldType(buffer, fieldType);
  }
}
