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

import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.util.converter.DayIntConverter;
import com.nus.cool.core.util.converter.StringIntConverter;
import java.util.List;

/**
 * Field Filter Factory
 *
 * @author zhongle, hongbin
 * @version 0.1
 * @since 0.1
 */
public class FieldFilterFactory {

  public static FieldFilter create(FieldSchema schema, List<String> values) {
    switch (schema.getFieldType()) {
      case AppKey:
      case UserKey:
      case Segment:
      case Action:
        return new SetFieldFilter(values);
      case ActionTime:
        return new RangeFieldFilter(values, new DayIntConverter());
      case Metric:
        return new RangeFieldFilter(values, new StringIntConverter());
      default:
        throw new IllegalArgumentException("Unsupported field type: " + schema.getFieldType());
    }
  }
}
