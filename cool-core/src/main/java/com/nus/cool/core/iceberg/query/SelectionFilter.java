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

package com.nus.cool.core.iceberg.query;

import com.nus.cool.core.cohort.filter.FieldFilter;
import java.util.ArrayList;
import java.util.List;

/**
 * Selection filter class.
 */
public class SelectionFilter {

  private SelectionQuery.SelectionType type;

  private FieldFilter filter;

  private String dimension;

  private List<SelectionFilter> fields = new ArrayList<>();

  public SelectionQuery.SelectionType getType() {
    return type;
  }

  public void setType(SelectionQuery.SelectionType type) {
    this.type = type;
  }

  public FieldFilter getFilter() {
    return filter;
  }

  public void setFilter(FieldFilter filter) {
    this.filter = filter;
  }

  public List<SelectionFilter> getFields() {
    return fields;
  }

  public void setFields(List<SelectionFilter> fields) {
    this.fields = fields;
  }

  public String getDimension() {
    return dimension;
  }

  public void setDimension(String dimension) {
    this.dimension = dimension;
  }
}
