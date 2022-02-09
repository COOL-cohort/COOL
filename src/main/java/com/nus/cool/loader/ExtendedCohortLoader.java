package com.nus.cool.loader;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.nus.cool.core.cohort.CohortQuery;
import com.nus.cool.core.cohort.ExtendedCohortQuery;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.storevector.InputVector;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExtendedCohortLoader {

    public static void main(String[] args) throws IOException {

        String testPath = args[0];
        String dataPath = args[1];
        String queryPath = args[2];

        CoolModel coolModel = new CoolModel(testPath);
        coolModel.reload(dataPath);

        // cohort query from json
        ObjectMapper mapper = new ObjectMapper();
        ExtendedCohortQuery query = mapper.readValue(new File(queryPath), ExtendedCohortQuery.class);

        System.out.println(" ------  checking query info ------ ");
        System.out.println(query);
        System.out.println(" ------  checking query info done ------ ");

        // Load cubes from outsource, which is output of previous query if any
        Map<String, DataOutputStream> inputCohortCuble = Maps.newHashMap();
        if (query.getInputCohort() != null) {
            File root = new File(dataPath, query.getInputCohort());
            File[] versions = root.listFiles(File::isDirectory);
            if (versions != null) {
                // for each directory
                for (File version : versions) {
                    File[] cubletFiles = version.listFiles((file, s) -> s.endsWith(".dz"));
                    if (cubletFiles != null) {
                        // for each file under such directory
                        for (File cubletFile : cubletFiles) {
                            inputCohortCuble.put(cubletFile.getName(),
                                    new DataOutputStream(new FileOutputStream(cubletFile, true)));
                        }
                    }
                }
            }
        }

        System.out.println(" ------  checking cube inputCohortCuble  ------ ");
        System.out.println(inputCohortCuble);
        System.out.println(" ------  checking cube inputCohortCuble done  ------ ");


        coolModel.getCube(query.getInputCohort()).get();

        InputVector userVector = coolModel.getCohortUsers(query.getInputCohort());


        List<ResultTuple> resultTuples = executeQuery(
                coolModel.getCube(query.getDataSource()), query, inputCohortCuble);
        QueryResult result = QueryResult.ok(resultTuples);
        System.out.println(result.toString());
        coolModel.close();

    }

    /**
     * ececute quety
     *
     * @param cube the cube that stores the data we need
     * @param query the cohort query needed to process
     * @return the result of the query
     */

    public QueryResult executeQuery(CubeRS cube, InputVector users, ExtendedCohortQuery query) {
        List<CubletRS> cublets = cube.getCublets();
        TableSchema tableSchema = cube.getTableSchema();
        CubeSchema cubeSchema = cube.getCubeSchema();
        List<ExtendedResultTuple> resultSet = new ArrayList<>();

        for (CubletRS cubletRS : cublets) {
            MetaChunkRS metaChunk = cubletRS.getMetaChunk();
            ExtendedCohortSelection sigma = new ExtendedCohortSelection();
            ExtendedCohortAggregation gamma = new ExtendedCohortAggregation(sigma);
            gamma.init(cubeSchema, tableSchema, users, query);
            gamma.process(metaChunk);
            if(sigma.isUserActiveCublet()) {
                List<ChunkRS> dataChunks = cubletRS.getDataChunks();
                for(ChunkRS dataChunk : dataChunks) {
                    gamma.process(dataChunk);
                }
            }
            IOUtils.cleanup(LOG, gamma);

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
                        int fieldID = tableSchema.getFieldID(cf.getField());
                        FieldSchema schema = tableSchema.getFieldSchema(fieldID);
                        if (schema.getDataType() == DataType.String) {
                            key.addDimensionName(metaChunk.getMetaField(fieldID).getString(key.getDimensions().get(cnt)));
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

        return QueryResult.ok(resultSet);
    }




}
