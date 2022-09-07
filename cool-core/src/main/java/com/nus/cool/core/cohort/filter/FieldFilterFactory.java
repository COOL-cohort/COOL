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
package com.nus.cool.core.cohort.filter;

import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.util.converter.DayIntConverter;
import com.nus.cool.core.util.converter.StringIntConverter;
import com.nus.cool.core.cohort.ExtendedFieldSet;
import java.util.List;

/**
 * FieldFilterFactory is used to create a filter when the field type is action, actiontime or metric
 */
public class FieldFilterFactory {
  /**
   * Create a filter according to the type of the field
   *
   * @param schema the schema of the field
   * @param values the values of the field
   */
  public static FieldFilter create(FieldSchema schema, ExtendedFieldSet fieldSet, List<String> values) {
    switch (schema.getFieldType()) {
      case AppKey:
      case UserKey:
      case Segment:
      case Action:
        return new SetFieldFilter(fieldSet, values, schema.getFieldType());
      case ActionTime:
        return new RangeFieldFilter(fieldSet, values, new DayIntConverter(),schema.getFieldType());
      case Metric:
        return new RangeFieldFilter(fieldSet, values, new StringIntConverter(),schema.getFieldType());
      default:
        throw new IllegalArgumentException("Unsupported field type: " + schema.getFieldType());
    }
  }
}
