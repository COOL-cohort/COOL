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
package com.nus.cool.core.util.converter;

/**
 *
 * @author zhongle
 * @version 0.1
 * @since 0.1
 */
public class StringIntConverter implements NumericConverter {

  @Override
  public int toInt(String v) {
    return Integer.parseInt(v);
  }

  @Override
  public String getString(int i) {
    return String.valueOf(i);
  }
}
