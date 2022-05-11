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

package com.nus.cool.model;

import com.nus.cool.core.cohort.*;
import com.nus.cool.core.cohort.funnel.FunnelProcess;
import com.nus.cool.core.cohort.funnel.FunnelQuery;
import com.nus.cool.core.io.compression.Compressor;
import com.nus.cool.core.io.compression.Histogram;
import com.nus.cool.core.io.compression.ZIntBitCompressor;
import com.nus.cool.core.io.readstore.*;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.DataType;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.reader.CoolTupleReader;
import com.nus.cool.core.util.writer.DataWriter;
import com.nus.cool.result.ExtendedResultTuple;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Execute different Query
 */
public class CoolCohortEngine {

    public List<String> listCohortUsers(CubeRS cube, List<Integer> inCohort){
        List<String> outCohort = new ArrayList<>();

        CubletRS cubletRS = cube.getCublets().get(0);
        MetaChunkRS metaChunk = cubletRS.getMetaChunk();

        TableSchema tableSchema = cube.getSchema();
        MetaFieldRS metaField = metaChunk.getMetaField(tableSchema.getUserKeyField(), FieldType.UserKey);
        for(Integer userID : inCohort){
            outCohort.add(metaField.getString(userID));
        }
        return outCohort;
    }

    /**
     * Cohort Users
     * @param cube cube
     * @param users users
     * @param query query
     * @return result
     * @throws IOException
     */
    public List<Integer> selectCohortUsers(CubeRS cube,
                                                  InputVector users,
                                                  ExtendedCohortQuery query) throws IOException {
        if (cube == null)
            throw new IOException("data source is null");

        List<CubletRS> cublets = cube.getCublets();
        TableSchema tableSchema = cube.getSchema();
        List<Integer> userList = new ArrayList<>();

        for (CubletRS cubletRS : cublets) {
            MetaChunkRS metaChunk = cubletRS.getMetaChunk();
            ExtendedCohortSelection sigma = new ExtendedCohortSelection();
            CohortUserSection gamma = new CohortUserSection(sigma);
            gamma.init(tableSchema, users, query);
            gamma.process(metaChunk);
            if(sigma.isUserActiveCublet()) {
                List<ChunkRS> dataChunks = cubletRS.getDataChunks();
                for(ChunkRS dataChunk : dataChunks) {
                    gamma.process(dataChunk);
                }
            }

            userList.addAll((List<Integer>)gamma.getCubletResults());
        }

        return userList;
    }


    /**
     * createCohort
     * @param users users
     * @param query query instance
     * @param cohortRoot file
     * @return result
     */
    public void createCohort(ExtendedCohortQuery query, List<Integer> users, File cohortRoot) throws IOException {
        String cohortName = query.getOutputCohort();
        File cohort = new File(cohortRoot, cohortName);

        cohort.createNewFile();

        DataOutputStream stream = new DataOutputStream(new FileOutputStream(cohort));
        int[] userArray = new int[users.size()];
        Iterator<Integer> iter = users.iterator();
        for (int i = 0; i < userArray.length; i++) {
            userArray[i] = iter.next().intValue();
        }

        int maxValue = Integer.MIN_VALUE;
        for(int v : userArray)
            if(v > maxValue) maxValue = v;

        Compressor compressor = new ZIntBitCompressor(
                Histogram.builder()
                        .max(maxValue)
                        .numOfValues(userArray.length)
                        .uniqueValues(userArray.length)
                        .build());
        byte[] compressed = new byte[compressor.maxCompressedLength()];
        int nbytes = compressor.compress(userArray, 0, userArray.length,
                compressed, 0, compressed.length);
        stream.write(compressed, 0, nbytes);
        stream.writeBytes(query.toString());
        stream.close();
    }

    public List<ExtendedResultTuple> processCohortResult(ExtendedCohortQuery query,
                                                         TableSchema tableSchema,
                                                         MetaChunkRS metaChunk,
                                                         ExtendedCohortSelection sigma,
                                                         Map<ExtendedCohort, Map<Integer, List<Double>>> resultMap){
        List<ExtendedResultTuple> resultSet = new ArrayList<>();
        for (Map.Entry<ExtendedCohort, Map<Integer, List<Double>>> entry:resultMap.entrySet()) {
            ExtendedCohort key = entry.getKey();

            // update dimension names
            List<BirthSequence.BirthEvent> events = query.getBirthSequence().getBirthEvents();
            int cnt = 0;
            for (int idx = 0; idx < events.size(); ++idx) {
                BirthSequence.BirthEvent be = events.get(idx);
                for (BirthSequence.CohortField cf : be.getCohortFields()) {
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

    /**
     * performCohortQuery
     * @param cube cube name
     * @param users users
     * @param query query instance
     * @return result
     */
    public List<ExtendedResultTuple> performCohortQuery(CubeRS cube, InputVector users, ExtendedCohortQuery query) {
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

    /**
     * performFunnelQuery
     * @param cube cube name
     * @param users users
     * @param query query instance
     * @return result
     */
    public int[] performFunnelQuery(CubeRS cube, InputVector users, FunnelQuery query){
        List<CubletRS> cublets = cube.getCublets();
        TableSchema tableSchema = cube.getTableSchema();
        int[] result = new int[query.getStages().size()];

        for (int i = 0; i < result.length; i++) {
            result[i] = 0;
        }

        for (CubletRS cubletRS : cublets) {
            MetaChunkRS metaChunk = cubletRS.getMetaChunk();
            FunnelProcess gamma = new FunnelProcess();
            gamma.init(tableSchema, users, query);
            gamma.process(metaChunk);
            List<ChunkRS> dataChunks = cubletRS.getDataChunks();
            for (ChunkRS dataChunk : dataChunks) {
                gamma.process(dataChunk);
            }

            int[] cubletResult = (int[]) gamma.getCubletResults();
            for (int i = 0; i < result.length; i++) {
                result[i] += cubletResult[i];
            }
        }

        return result;
    }

    public boolean exportCohort(CubeRS cube, InputVector users, DataWriter writer) throws IOException {
        CoolTupleReader reader = new CoolTupleReader(cube, users);
        writer.Initialize();
        while (reader.hasNext()) {
            writer.Add(reader.next());
        }
        writer.Finish();
        reader.close();
        return true;
    }
}
