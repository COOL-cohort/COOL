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

package com.nus.cool.core.cohort;

import java.util.ArrayList;
import java.util.List;

/**
 * Field is mainly used to represent the conditions we set up for extended queries.
 *
 */
public class ExtendedFieldSet {

  public static enum FieldValueType {

    AbsoluteValue,

    IncreaseByAbsoluteValue,

    IncreaseByPercentage,

    Equal,

    Inequal

  }

  public static enum FieldSetType {

    Set,

    Range

  }

  public static class FieldValue {

    private FieldValueType type = FieldValueType.AbsoluteValue;

    private List<String> values = new ArrayList<>(0);

    private String baseField;

    private int baseEvent = -1;

    public FieldValueType getType() {
      return type;
    }

    public List<String> getValues() {
      return values;
    }

    public String getBaseField() {
      return baseField;
    }

    public int getBaseEvent() {
      return baseEvent;
    }
  }

  private FieldSetType setType;

  private String field;

  private FieldValue fieldValue;

  public void setFilterType(FieldSetType setType) {
    this.setType = setType;
  }

  public String getCubeField() {
    return field;
  }

  public void setCubeField(String field) {
    this.field = field;
  }

  public FieldValue getFieldValue() {
    return fieldValue;
  }
}
