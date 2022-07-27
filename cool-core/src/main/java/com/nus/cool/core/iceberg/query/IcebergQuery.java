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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

@Data
public class IcebergQuery {

    public enum granularityType{
        DAY,
        MONTH,
        YEAR,
        NULL
    }

    private static final Log LOG = LogFactory.getLog(IcebergQuery.class);
    // dataSource path
    private String dataSource;
    // select condition
    private SelectionQuery selection;
    // a list a groupFields
    private List<String> groupFields;
    // a list of aggregation functions, sum, count etc
    private List<Aggregation> aggregations;
    // selected time range
    private String timeRange;

    // granularity for time range
    private granularityType granularity;
    // granularity for groupBy, if the groupBy field is dataType,
    private granularityType groupFields_granularity;

    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String toPrettyString() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            LOG.info(e);
        }
        return null;
    }

    public static IcebergQuery read(InputStream in) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(in, IcebergQuery.class);
    }
}