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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;


public class IcebergQuery {

    public enum granularityType{
        DAY,

        MONTH,

        YEAR,

        NULL
    }

    private static final Log LOG = LogFactory.getLog(IcebergQuery.class);

    private String dataSource;

    private SelectionQuery selection;

    private List<String> groupFields;

    private List<Aggregation> aggregations;

    private String timeRange;

    private granularityType granularity;

    public String getTimeRange() {
        return timeRange;
    }

    public void setTimeRange(String timeRange) {
        this.timeRange = timeRange;
    }

    public List<Aggregation> getAggregations() {
        return aggregations;
    }

    public void setAggregations(List<Aggregation> aggregations) {
        this.aggregations = aggregations;
    }

    public List<String> getGroupFields() {
        return groupFields;
    }

    public void setGroupFields(List<String> groupFields) {
        this.groupFields = groupFields;
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public SelectionQuery getSelection() {
        return selection;
    }

    public void setSelection(SelectionQuery selection) {
        this.selection = selection;
    }

    public granularityType getGranularity() {
        return granularity;
    }

    public void setGranularity(granularityType granularity) {
        this.granularity = granularity;
    }

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