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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.Data;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

@Data
public class ExtendedCohortQuery {

    @Data
    public static class AgeField {

        private String field;

        private TimeUnit unit = TimeUnit.DAY;

        private int ageInterval = 1;

        private List<ExtendedFieldSet> eventSelection = new ArrayList<>();

        private List<String> range;

        private boolean fillWithLastObserved = false;

        private boolean fillWithNextObserved = false;

        public boolean isFillWithNextObserved() {
            return fillWithNextObserved;
        }

        public void setFillWithNextObserved(boolean fillWithNextObserved) {
            this.fillWithNextObserved = fillWithNextObserved;
        }
    }

    private static final Log LOG = LogFactory.getLog(ExtendedCohortQuery.class);

    private String dataSource;

    private String appKey;

    private BirthSequence birthSequence = new BirthSequence();

    private AgeField ageField = null;

    private List<ExtendedFieldSet> ageSelection = null;

    private String measure;

    private String inputCohort;

    private String outputCohort;

    private String userId;

    @JsonIgnore
    public boolean isValid() {
        return (birthSequence != null) &&
                (birthSequence.isValid()) &&
                (dataSource != null) &&
                (ageField != null) &&
                (measure != null);
    }

    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            LOG.info(e);
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

    /**
     * Read query from InputStream
     * @param in InputStream
     * @return query instance
     * @throws IOException
     */
    public static ExtendedCohortQuery read(InputStream in) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(in, ExtendedCohortQuery.class);
    }

}
