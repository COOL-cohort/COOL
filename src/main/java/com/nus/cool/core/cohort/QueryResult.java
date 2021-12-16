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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;

/**
 * @author zhongle, hongbin
 * @version 0.1
 * @since 0.1
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
public class QueryResult {

  private QueryStatus status;
  private String errMsg;
  private long elapsed;
  private Object result;

  public QueryResult(QueryStatus status, String errMsg, Object result) {
    this.status = status;
    this.errMsg = errMsg;
    this.result = result;
  }

  public static QueryResult ok(Object result) {
    return new QueryResult(QueryStatus.OK, null, result);
  }

  @Override
  public String toString() {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  public enum QueryStatus {
    OK,

    ERROR
  }
}
