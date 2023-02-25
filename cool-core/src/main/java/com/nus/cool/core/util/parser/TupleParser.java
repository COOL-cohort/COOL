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

package com.nus.cool.core.util.parser;

import com.nus.cool.core.field.FieldValue;
import java.io.IOException;

/**
 * TupleParser is a parser to parse a tuple to array.
 */
public interface TupleParser {

  /**
   * Parse tuple to array.
   *
   * @param tuple target tuple
   * @return string array
   */
  FieldValue[] parse(Object tuple) throws IOException;
}
