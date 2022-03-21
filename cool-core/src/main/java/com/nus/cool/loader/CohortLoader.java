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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.nus.cool.core.cohort.CohortAggregation;
import com.nus.cool.core.cohort.CohortKey;
import com.nus.cool.core.cohort.CohortQuery;
import com.nus.cool.core.cohort.CohortSelection;
import com.nus.cool.core.cohort.FieldSet;
import com.nus.cool.core.cohort.QueryResult;
import com.nus.cool.core.io.compression.SimpleBitSetCompressor;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.readstore.CubletRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.IntegerUtil;
import com.nus.cool.core.util.converter.DayIntConverter;
import com.nus.cool.core.util.converter.NumericConverter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * CohortLoader is a higher level abstraction for cohort query
 * With CohortLoader, you can easily get the query result
 */
public class CohortLoader {

  /**
   * Local model for cool
   *
   * @param args [0] the output data dir (eg, dir of .dz fiel)
   *        args [1] application name, also the folder name under above folder
   *        args [2] query's path, eg sogamo/query0.json
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {

    String datasetPath = args[0];
    String appPath = args[1];
    String queryPath = args[2];

    CoolModel coolModel = new CoolModel(datasetPath);
    coolModel.reload(appPath);

    // cohort query from json
    ObjectMapper mapper = new ObjectMapper();
    CohortQuery query = mapper.readValue(new File(queryPath), CohortQuery.class);

    System.out.println(" ------  checking query info ------ ");
    System.out.println(query);
    System.out.println(" ------  checking query info done ------ ");

    // Load cubes from outsource, which is output of previous query if any
    Map<String, DataOutputStream> outSourceMap = Maps.newHashMap();
    if (query.getOutSource() != null) {
      File root = new File(appPath, query.getOutSource());
      File[] versions = root.listFiles(File::isDirectory);
      if (versions != null) {
        // for each directory
        for (File version : versions) {
          File[] cubletFiles = version.listFiles((file, s) -> s.endsWith(".dz"));
          if (cubletFiles != null) {
            // for each file under such directory
            for (File cubletFile : cubletFiles) {
              outSourceMap.put(cubletFile.getName(),
                      new DataOutputStream(new FileOutputStream(cubletFile, true)));
            }
          }
        }
      }
    }

    System.out.println(" ------  checking outSourceMap  ------ ");
    System.out.println(outSourceMap);
    System.out.println(" ------  checking outSourceMap done  ------ ");

    List<ResultTuple> resultTuples = executeQuery(
            coolModel.getCube(query.getDataSource()), query,
            outSourceMap);
    QueryResult result = QueryResult.ok(resultTuples);
    System.out.println(result.toString());
    coolModel.close();
  }

  /**
   * execute query
   * 
   * @param cube the cube that stores the data we need
   * @param query the cohort query needed to process
   * @param map the cublet and it's data
   * @return the result of the query
   */
  public static List<ResultTuple> executeQuery(CubeRS cube, CohortQuery query,
      Map<String, DataOutputStream> map) throws IOException {
    List<CubletRS> cublets = cube.getCublets();
    TableSchema schema = cube.getSchema();
    List<ResultTuple> resultSet = Lists.newArrayList();
    boolean tag = query.getOutSource() != null;
    List<BitSet> bitSets = Lists.newArrayList();
    // process each cublet
    for (CubletRS cublet : cublets) {
      MetaChunkRS metaChunk = cublet.getMetaChunk();
      // init cohort seleciton
      CohortSelection sigma = new CohortSelection();
      // init Aggregation
      CohortAggregation gamma = new CohortAggregation(sigma);
      gamma.init(schema, query);
      gamma.process(metaChunk);
      if (sigma.isBUserActiveCublet()) {
        List<ChunkRS> dataChunks = cublet.getDataChunks();
        for (ChunkRS dataChunk : dataChunks) {
          gamma.process(dataChunk);
          bitSets.add(gamma.getBs());
        }
      }
      if (tag) {
        int end = cublet.getLimit();
        DataOutputStream out = map.get(cublet.getFile());
          for (BitSet bs : bitSets) {
              SimpleBitSetCompressor.compress(bs, out);
          }
        out.writeInt(IntegerUtil.toNativeByteOrder(end));
        out.writeInt(IntegerUtil.toNativeByteOrder(bitSets.size()));
        out.writeInt(IntegerUtil.toNativeByteOrder(0));
      }

      String cohortField = query.getCohortFields()[0];
      String actionTimeField = schema.getActionTimeFieldName();
      NumericConverter converter =
          cohortField.equals(actionTimeField) ? new DayIntConverter() : null;
      MetaFieldRS cohortMetaField = metaChunk.getMetaField(cohortField);
      Map<CohortKey, Long> results = gamma.getCubletResults();
      for (Map.Entry<CohortKey, Long> entry : results.entrySet()) {
        CohortKey key = entry.getKey();
        int cId = key.getCohort();
        String cohort = converter == null ? cohortMetaField.getString(key.getCohort())
            : converter.getString(cId);
        int age = key.getAge();
        long measure = entry.getValue();
        resultSet.add(new ResultTuple(cohort, age, measure));
      }
    }
    // merge the partial query results
    return ResultTuple.merge(resultSet);
  }
}
