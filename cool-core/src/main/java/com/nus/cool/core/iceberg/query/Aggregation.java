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

import java.util.List;

import com.nus.cool.core.iceberg.aggregator.AggregatorFactory;

/**
 * Aggregator query structure defines a set of aggregators on a field.
 */
public class Aggregation {
  private String fieldName;

  private List<AggregatorFactory.AggregatorType> operators;

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  public List<AggregatorFactory.AggregatorType> getOperators() {
    return operators;
  }

  public void setOperators(List<AggregatorFactory.AggregatorType> operators) {
    this.operators = operators;
  }

  public Aggregation(String fieldName, List<AggregatorFactory.AggregatorType> operators) {
    this.fieldName = fieldName;
    this.operators = operators;
  }

  public Aggregation() {
  }
}
