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
package com.nus.cool.core.cohort.filter;

import static com.google.common.base.Preconditions.checkNotNull;

import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.util.ArrayUtil;
import java.util.BitSet;
import java.util.List;

/**
 * @author zhongle, hongbin
 * @version 0.1
 * @since 0.1
 */
public class SetFieldFilter implements FieldFilter {

  private List<String> values;

  private boolean isAll;

  private int[] cubeIDs;

  private BitSet filter;

  private InputVector chunkValues;

  public SetFieldFilter(List<String> values) {
    this.values = checkNotNull(values);
    this.isAll = this.values.contains("ALL");
    this.cubeIDs = this.isAll ? new int[2] : new int[values.size()];
  }

  @Override
  public int getMinKey() {
    return ArrayUtil.min(this.cubeIDs);
  }

  @Override
  public int getMaxKey() {
    return ArrayUtil.max(this.cubeIDs);
  }

  @Override
  public boolean accept(MetaFieldRS metaField) {
    if (this.isAll) {
      this.cubeIDs[1] = metaField.count() - 1;
      return true;
    }
    boolean bHit = false;
    int i = 0;
    for (String v : this.values) {
      int tmp = metaField.find(v);
      cubeIDs[i++] = tmp;
      bHit |= (tmp >= 0);
    }
    return bHit || (this.values.isEmpty());
  }

  @Override
  public boolean accept(FieldRS field) {
      if (this.isAll) {
          return true;
      }

    InputVector keyVec = field.getKeyVector();
    this.filter = new BitSet(keyVec.size());
    this.chunkValues = field.getValueVector();

    boolean bHit = false;
    for (int cubeId : this.cubeIDs) {
      if (cubeId >= 0) {
        int tmp = keyVec.find(cubeId);
        bHit |= (tmp >= 0);
          if (tmp >= 0) {
              this.filter.set(tmp);
          }
      }
    }
    return bHit || (this.values.isEmpty());
  }

  @Override
  public boolean accept(int v) {
      if (this.isAll) {
          return true;
      }
    return this.filter.get(v);
  }

  @Override
  public List<String> getValues() {
    return this.values;
  }
}
