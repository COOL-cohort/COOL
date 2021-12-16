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
package com.nus.cool.core.cohort;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Maps;
import com.nus.cool.core.cohort.filter.FieldFilter;
import com.nus.cool.core.cohort.filter.FieldFilterFactory;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import lombok.Getter;

/**
 * @author zhongle, hongbin
 * @version 0.1
 * @since 0.1
 */
public class CohortSelection implements Operator {

  private TableSchema schema;

  @Getter
  private boolean bUserActiveCublet;

  private boolean bAgeActiveCublet;

  @Getter
  private boolean bUserActiveChunk;

  private boolean bAgeActiveChunk;

  @Getter
  private FieldFilter appFilter;

  private Map<String, FieldFilter> birthFilters = Maps.newHashMap();

  private Map<String, FieldFilter> ageFilters = Maps.newHashMap();

  private Map<String, FieldRS> birthFilterFields = Maps.newHashMap();

  private Map<String, FieldRS> ageFilterFields = Maps.newHashMap();

  @Override
  public void init(TableSchema schema, CohortQuery query) {
    this.schema = checkNotNull(schema);
    checkNotNull(query);

    String app = query.getAppKey();
    this.appFilter = FieldFilterFactory
        .create(this.schema.getField(this.schema.getAppKeyField()), Arrays.asList(app));

    List<FieldSet> birthSelectors = query.getBirthSelection();
    for (FieldSet fs : birthSelectors) {
      String fieldName = fs.getField();
      this.birthFilters.put(fieldName,
          FieldFilterFactory.create(this.schema.getField(fieldName), fs.getValues()));
    }

    List<FieldSet> ageSelectors = query.getAgeSelection();
    for (FieldSet fs : ageSelectors) {
      String fieldName = fs.getField();
      this.ageFilters.put(fieldName,
          FieldFilterFactory.create(this.schema.getField(fieldName), fs.getValues()));
    }
  }

  @Override
  public void process(MetaChunkRS metaChunk) {
    this.bUserActiveCublet = true;
    this.bAgeActiveCublet = true;

    boolean bAccept = this.appFilter
        .accept(metaChunk.getMetaField(this.schema.getAppKeyField(), FieldType.AppKey));
    if (!bAccept) {
      this.bUserActiveCublet = false;
      return;
    }

    for (Map.Entry<String, FieldFilter> entry : this.birthFilters.entrySet()) {
      MetaFieldRS metaField = metaChunk.getMetaField(entry.getKey());
      this.bUserActiveCublet &= entry.getValue().accept(metaField);
    }

    for (Map.Entry<String, FieldFilter> entry : this.ageFilters.entrySet()) {
      MetaFieldRS metaField = metaChunk.getMetaField(entry.getKey());
      this.bAgeActiveCublet &= entry.getValue().accept(metaField);
    }
  }

  @Override
  public void process(ChunkRS chunk) {
    this.birthFilterFields.clear();
    this.ageFilterFields.clear();
    this.bUserActiveChunk = true;
    this.bAgeActiveChunk = true;

    boolean bAccept = this.appFilter.accept(chunk.getField(this.schema.getAppKeyField()));
    if (!bAccept) {
      this.bUserActiveChunk = false;
      return;
    }

    for (Map.Entry<String, FieldFilter> entry : this.birthFilters.entrySet()) {
      FieldRS field = chunk.getField(this.schema.getFieldID(entry.getKey()));
      this.bUserActiveChunk &= entry.getValue().accept(field);
      this.birthFilterFields.put(entry.getKey(), field);
    }

    for (Map.Entry<String, FieldFilter> entry : this.ageFilters.entrySet()) {
      FieldRS field = chunk.getField(this.schema.getFieldID(entry.getKey()));
      this.bAgeActiveChunk &= entry.getValue().accept(field);
      this.ageFilterFields.put(entry.getKey(), field);
    }
  }

  public FieldFilter getBirthFieldFilter(String fieldName) {
    return this.birthFilters.get(fieldName);
  }

  public boolean selectUser(int birthOff) {
    boolean bSelected = true;
    for (Map.Entry<String, FieldFilter> entry : this.birthFilters.entrySet()) {
      FieldRS field = this.birthFilterFields.get(entry.getKey());
      InputVector fieldInput = field.getValueVector();
      fieldInput.skipTo(birthOff);
      bSelected = entry.getValue().accept(fieldInput.next());
        if (!bSelected) {
            break;
        }
    }
    return bSelected;
  }

  public void selectAgeActivities(int ageOff, int ageEnd, BitSet bs) {
    for (Map.Entry<String, FieldFilter> entry : this.ageFilters.entrySet()) {
      FieldRS field = this.ageFilterFields.get(entry.getKey());
      InputVector fieldInput = field.getValueVector();
      fieldInput.skipTo(ageOff);
      FieldFilter ageFilter = entry.getValue();
      if ((bs.cardinality() << 1) >= (ageEnd - ageOff)) {
        for (int i = ageOff; i < ageEnd; i++) {
            if (!ageFilter.accept(fieldInput.next())) {
                bs.clear(i);
            }
        }
      } else {
        int off = bs.nextSetBit(ageOff);
        while (off < ageEnd && off >= 0) {
          fieldInput.skipTo(off);
            if (!ageFilter.accept(fieldInput.next())) {
                bs.clear(off);
            }
          off = bs.nextSetBit(off + 1);
        }
      }
    }
  }
}
