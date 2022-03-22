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
package com.nus.cool.loader;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.BirthSequence.BirthEvent;
import com.nus.cool.core.cohort.BirthSequence.CohortField;
import com.nus.cool.core.cohort.ExtendedCohort;
import com.nus.cool.core.cohort.ExtendedCohortQuery;
import com.nus.cool.core.cohort.ExtendedCohortSelection;
import com.nus.cool.core.cohort.ExtendedCohortAggregation;
import com.nus.cool.core.io.readstore.*;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.DataType;
import com.nus.cool.model.CoolModel;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ExtendedCohortLoader is a higher level abstraction for cohort query.
 * With ExtendedCohortLoader, you can easily get the cohort query result
 */
public class ExtendedCohortLoader {

    /**
     * Local model for cool
     *
     * @param args [0] dataset path: the path to all datasets, e.g., test
     *        args [1] application path: the path to the application dataset that is under above folder, e.g., health
     *        args [2] query path: the path to the cohort query, e.g., health/query2.json
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        String datasetPath = args[0];
        String appPath = args[1];
        String queryPath = args[2];

        // reload the dataset of the application
        CoolModel coolModel = new CoolModel(datasetPath);
        coolModel.reload(appPath);

        // load the cohort query from a json file
        ObjectMapper mapper = new ObjectMapper();
        ExtendedCohortQuery query = mapper.readValue(new File(queryPath), ExtendedCohortQuery.class);

        System.out.println(" ------  checking query info ------ ");
        System.out.println(query);
        System.out.println(" ------  checking query info done ------ ");

        if(!query.isValid())
            throw new IOException("Wrong cohort query");

        System.out.println("Get cube:" + coolModel.getCube(query.getDataSource()));

        long start = System.currentTimeMillis();
        System.out.println("Input Cohort: " + query.getInputCohort());
        String inputCohorts = query.getInputCohort();
        if (inputCohorts!=null){
            coolModel.loadCohorts(inputCohorts, datasetPath+"/"+appPath);
        }

        // preprocess on the selected cohort.
        InputVector userVector = coolModel.getCohortUsers(query.getInputCohort());
        System.out.println(userVector);

        List<ExtendedResultTuple> result = executeQuery(coolModel.getCube(query.getDataSource()),userVector, query);
        System.out.println(" result for query1 is  " + result);
        long end = System.currentTimeMillis();
        System.out.println("Exe time: " + (end - start));
    }

    public static List<ExtendedResultTuple> processCohortResult(ExtendedCohortQuery query,
                                                                TableSchema tableSchema,
                                                                MetaChunkRS metaChunk,
                                                                ExtendedCohortSelection sigma,
                                                                Map<ExtendedCohort, Map<Integer, List<Double>>> resultMap){
        List<ExtendedResultTuple> resultSet = new ArrayList<>();
        for (Map.Entry<ExtendedCohort, Map<Integer, List<Double>>> entry:resultMap.entrySet()) {
            ExtendedCohort key = entry.getKey();

            // update dimension names
            List<BirthEvent> events = query.getBirthSequence().getBirthEvents();
            int cnt = 0;
            for (int idx = 0; idx < events.size(); ++idx) {
                BirthEvent be = events.get(idx);
                for (CohortField cf : be.getCohortFields()) {
                    String fieldName = cf.getField();
                    int fieldID = tableSchema.getFieldID(cf.getField());
                    FieldSchema schema = tableSchema.getField(fieldID);
                    if (schema.getDataType() == DataType.String) {
                        key.addDimensionName(metaChunk.getMetaField(fieldName).getString(key.getDimensions().get(cnt)));
                    } else {
                        MetaFieldRS filed = metaChunk.getMetaField(fieldName);
                        StringBuilder builder = new StringBuilder();
                        builder.append("(");
                        int level = key.getDimensions().get(cnt);
                        if (cf.getMinLevel() == level) {
                            builder.append(Integer.toString(filed.getMinValue())+", ");
                        } else {
                            if (cf.isLogScale()) {
                                builder.append((new Double(Math.pow(cf.getScale(), level - 1))).longValue());
                            } else {
                                builder.append((new Double(cf.getScale() * (level - 1))).longValue());
                            }
                            builder.append(", ");
                        }

                        if (cf.getMinLevel() + cf.getNumLevel() == level) {
                            builder.append(Integer.toString(filed.getMaxValue())+")");
                        } else {
                            if (cf.isLogScale()) {
                                builder.append((new Double(Math.pow(cf.getScale(), level))).longValue());
                            } else {
                                builder.append((new Double(cf.getScale() * level)).longValue());
                            }
                            builder.append("]");
                        }
                        key.addDimensionName(builder.toString());
                    }
                    ++cnt;
                }
            }

            for (Map.Entry<Integer, List<Double>> ageMetric : entry.getValue().entrySet()) {
                int age = ageMetric.getKey();
                if (!sigma.getAgeFieldFilter().accept(age))
                    continue;
                double measure = ageMetric.getValue().get(0);
                double min = 0.0;
                double max = 0.0;
                double sum = 0.0;
                double num = 0.0;
                if (ageMetric.getValue().size() >= 5) {
                    min = ageMetric.getValue().get(1);
                    max = ageMetric.getValue().get(2);
                    sum = ageMetric.getValue().get(3);
                    num = ageMetric.getValue().get(4);
                }
                if (num==0) {
                    min = 0.0;
                    max = 0.0;
                }
                resultSet.add(new ExtendedResultTuple(key.toString(), age, measure, min, max, sum, num));
            }
        }
        return resultSet;
    }

    public static List<ExtendedResultTuple> executeQuery(CubeRS cube, InputVector users, ExtendedCohortQuery query) {
        List<CubletRS> cublets = cube.getCublets();
        TableSchema tableSchema = cube.getTableSchema();
        List<ExtendedResultTuple> resultSet = new ArrayList<>();

        for (CubletRS cubletRS : cublets) {
            MetaChunkRS metaChunk = cubletRS.getMetaChunk();
            ExtendedCohortSelection sigma = new ExtendedCohortSelection();
            ExtendedCohortAggregation gamma = new ExtendedCohortAggregation(sigma);

            // Initialize the birth events and trigger times
            gamma.init(tableSchema, users, query);
            // Check the validity of fields (e.g., the existences of field names and their values)
            gamma.process(metaChunk);
            if (sigma.isUserActiveCublet()) {
                List<ChunkRS> dataChunks = cubletRS.getDataChunks();
                for (ChunkRS dataChunk : dataChunks) {
                    gamma.process(dataChunk);
                }
            }

            Object results = gamma.getCubletResults();

            resultSet.addAll(processCohortResult(query,tableSchema,metaChunk,sigma,(Map<ExtendedCohort, Map<Integer, List<Double>>>) results));
        }

        return resultSet;
    }
}
