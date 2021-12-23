/*
 * Copyright 2021 Cool Squad Team
 *
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
package com.nus.cool.core.cohort.filter;

import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import java.util.List;

/**
 * FieldFilter is used as a filter of the fields, especially useful when setting up the birth condition and other selections
 */
public interface FieldFilter {

  /**
   * Get the global minimum of the conditions
   * 
   * @return the global minimum 
   */
  int getMinKey();

  /**
   * Get the global maximum of the conditions
   * 
   * @return the global maximum 
   */
  int getMaxKey();

  /**
   * Indicate whether the metafiled is eligible
   * 
   * @param metaField the metafield to be checked
   * @return false indicates the metafield is not eligible and true indicates the metafield is eligible
   */
  boolean accept(MetaFieldRS metaField);

  /**
   * Indicate whether the filed is eligible
   * 
   * @param field the field to be checked
   * @return false indicates the field is not eligible and true indicates the field is eligible
   */
  boolean accept(FieldRS field);

  /**
   * Indicate whether the interger v is eligible
   * 
   * @param v the interger to be checked
   * @return false indicates the interger is not eligible and true indicates the interger is eligible
   */
  boolean accept(int v);

  /**
   * Get the conditions set up before
   * 
   * @return the conditions and the minimum and maximum are separated by '|'
   */
  List<String> getValues();
}
