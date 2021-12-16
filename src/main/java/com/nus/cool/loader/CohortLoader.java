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
package com.nus.cool.loader;

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
 *
 * @author hongbin, zhongle
 * @version 0.1
 * @since 0.1
 */
public class CohortLoader {

  private static CoolModel coolModel;

  public static void main(String[] args) throws IOException {
    coolModel = new CoolModel(args[0]);
    coolModel.reload(args[1]);

//        ObjectMapper mapper = new ObjectMapper();
//        CohortQuery query = mapper.readValue(new File("query.json"), CohortQuery.class);

    CohortQuery query = new CohortQuery();
    query.setDataSource("sogamo");
    query.setAgeInterval(1);
    query.setMetric("Retention");
    String[] cohortFields = {"country"};
    query.setCohortFields(cohortFields);
    List<FieldSet> birthSelection = new ArrayList<>();
    List<String> values = new ArrayList<>();
    values.add("2013-05-20|2013-05-20");
    FieldSet fieldSet = new FieldSet(FieldSet.FieldSetType.Set, "eventDay", values);
    birthSelection.add(fieldSet);
    query.setBirthSelection(birthSelection);
    query.setBirthActions(new String[]{"launch"});
    query.setAppKey("fd1ec667-75a4-415d-a250-8fbb71be7cab");

    Map<String, DataOutputStream> map = Maps.newHashMap();
    if (query.getOutSource() != null) {
      File root = new File("cube/", query.getOutSource());
      File[] versions = root.listFiles(new FileFilter() {
        @Override
        public boolean accept(File file) {
          return file.isDirectory();
        }
      });
      for (File version : versions) {
        File[] cubletFiles = version.listFiles(new FilenameFilter() {
          @Override
          public boolean accept(File file, String s) {
            return s.endsWith(".dz");
          }
        });
          for (File cubletFile : cubletFiles) {
              map.put(cubletFile.getName(),
                  new DataOutputStream(new FileOutputStream(cubletFile, true)));
          }
      }
    }

    List<ResultTuple> resultTuples = executeQuery(coolModel.getCube(query.getDataSource()), query,
        map);
    QueryResult result = QueryResult.ok(resultTuples);
    System.out.println(result.toString());
    coolModel.close();
  }

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
      CohortSelection sigma = new CohortSelection();
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
