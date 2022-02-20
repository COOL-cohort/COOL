package com.nus.cool.queryserver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.nus.cool.core.cohort.*;
import com.nus.cool.core.cohort.BirthSequence.BirthEvent;
import com.nus.cool.core.cohort.BirthSequence.CohortField;
import com.nus.cool.core.cohort.QueryResult;
import com.nus.cool.core.io.compression.Compressor;
import com.nus.cool.core.io.compression.Histogram;
import com.nus.cool.core.io.compression.ZIntBitCompressor;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.readstore.CubletRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.schema.CubeSchema;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.DataType;
import com.nus.cool.loader.ExtendedResultTuple;

import javax.management.Query;
import javax.ws.rs.core.Response;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class QueryExecutor {
    public QueryExecutor(){}

    public List<Integer> selectCohortUsers(CubeRS cube, InputVector users, ExtendedCohortQuery query) throws IOException {
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

    public void createCohort(ExtendedCohortQuery query, List<Integer> users, String outPath) throws IOException {

        String cohortName = query.getOutputCohort();
        File cohort = new File(new File(outPath), cohortName);

        cohort.createNewFile();

        DataOutputStream stream =
                new DataOutputStream(new FileOutputStream(cohort));
        int[] userArray = new int[users.size()];
        Iterator<Integer> iter = users.iterator();
        for (int i = 0; i < userArray.length; i++) {
            userArray[i] = iter.next().intValue();
        }
        Compressor compressor = new ZIntBitCompressor(
                Histogram.builder()
                        .max(max(userArray))
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

    public static int max(int[] vec) {
        int max = Integer.MIN_VALUE;

        for(int v : vec)
            if(v > max)
                max = v;

        return max;
    }

    @SuppressWarnings("unchecked")
    public List<ExtendedResultTuple> executeCohortQuery(CubeRS cube, InputVector users, ExtendedCohortQuery query) {
        List<CubletRS> cublets = cube.getCublets();
        TableSchema tableSchema = cube.getTableSchema();
        List<ExtendedResultTuple> resultSet = new ArrayList<>();
        System.out.println(users.size());
        for (CubletRS cubletRS : cublets) {
            MetaChunkRS metaChunk = cubletRS.getMetaChunk();
            ExtendedCohortSelection sigma = new ExtendedCohortSelection();
            ExtendedCohortAggregation gamma = new ExtendedCohortAggregation(sigma);
            gamma.init(tableSchema, users, query);
            gamma.process(metaChunk);
            if (sigma.isUserActiveCublet()) {
                List<ChunkRS> dataChunks = cubletRS.getDataChunks();
                for (ChunkRS dataChunk : dataChunks) {
                    gamma.process(dataChunk);
                }
            }

            Object results = gamma.getCubletResults();

            Map<ExtendedCohort, Map<Integer, List<Double>>> resultMap =
                    (Map<ExtendedCohort, Map<Integer, List<Double>>>) results;

            for (Map.Entry<ExtendedCohort, Map<Integer, List<Double>>> entry :
                    resultMap.entrySet()) {
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
                            StringBuilder builder = new StringBuilder();
                            builder.append("(");
                            int level = key.getDimensions().get(cnt);
                            if (cf.getMinLevel() == level) {
                                builder.append("-inf, ");
                            } else {
                                if (cf.isLogScale()) {
                                    builder.append((new Double(Math.pow(cf.getScale(), level - 1))).longValue());
                                } else {
                                    builder.append((new Double(cf.getScale() * (level - 1))).longValue());
                                }
                                builder.append(", ");
                            }

                            if (cf.getMinLevel() + cf.getNumLevel() == level) {
                                builder.append("inf)");
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
                    resultSet.add(new ExtendedResultTuple(key.toString(), age, measure, min, max, sum, num));
                }
            }
        }
        System.out.println(resultSet);
        return resultSet;
    }

}
