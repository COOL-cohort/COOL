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

package com.nus.cool.core.cohort.refactor.olapSelect;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.nus.cool.core.cohort.refactor.filter.Filter;
import com.nus.cool.core.cohort.refactor.filter.FilterLayout;
import com.nus.cool.core.cohort.refactor.filter.FilterType;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import lombok.Data;

@Data
public class olapSelectionLayout {

  // inner data type for selection
  public enum SelectionType {
    and, or, filter
  }

  // what type of selection
  @JsonProperty("filter_type")
  private SelectionType type;

  // which dimension to use
  @JsonProperty("fieldSchema")
  private String dimension;

  // dimension type
  @JsonProperty("type")
  private FilterType dimension_type;

  // values accepted in this field
  @JsonProperty("acceptValue")
  private List<String> values;

  // sub-operation
  @JsonProperty("fields")
  private List<olapSelectionLayout> fields = new ArrayList<>();

  // each selection layout has one filter,
  private Filter filter;

  // recursively generate filter instance for this sub selection
  public void initSelectionFilter(MetaChunkRS metaChunk) {
    if (this.type.equals(SelectionType.filter)) {
      FilterLayout selectionFilter = new FilterLayout(this.dimension_type == FilterType.Set,
          this.values.toArray(new String[0]), null);
      selectionFilter.setFieldSchema(this.dimension);
      this.filter = selectionFilter.generateFilter();
      this.filter.loadMetaInfo(metaChunk);
    } else {
      for (olapSelectionLayout childSelection : this.fields) {
        childSelection.initSelectionFilter(metaChunk);
      }
    }
  }

  public HashSet<String> getSchemaSet() {
    HashSet<String> res = new HashSet<>();
    getSchemaSet(res);
    return res;
  }

  private void getSchemaSet(HashSet<String> res) {
    if (this.type.equals(SelectionType.filter)) {
      res.add(dimension);
    } else {
      for (olapSelectionLayout childSelection : this.fields) {
        childSelection.getSchemaSet(res);
      }
    }
  }

}
