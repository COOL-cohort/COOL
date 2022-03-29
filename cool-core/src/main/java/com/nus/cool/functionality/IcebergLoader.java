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

package com.nus.cool.functionality;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.QueryResult;
import com.nus.cool.core.iceberg.query.Aggregation;
import com.nus.cool.core.iceberg.query.IcebergAggregation;
import com.nus.cool.core.iceberg.query.IcebergQuery;
import com.nus.cool.core.iceberg.query.IcebergSelection;
import com.nus.cool.core.iceberg.result.BaseResult;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.readstore.CubletRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.model.CoolModel;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * IcebergLoader is a higher level abstraction for iceberg query
 * With IcebergLoader, you can easily get the query result
 */
public class IcebergLoader {

    /**
     * IcebergLoader model for cool
     *
     * @param args [0] the output data dir (eg, dir of .dz file)
     *        args [1] application name, also the folder name under above folder
     *        args [2] query's path, eg sogamo/query0.json
     * @throws IOException
     */
    public static void main(String[] args) throws IOException{

        // the path of dz file eg "COOL/cube"
        String dzFilePath = args[0];
        // query path  eg. tmp/2/query.json
        String queryFilePath = args[1];

        // load query
        ObjectMapper mapper = new ObjectMapper();
        IcebergQuery query = mapper.readValue(new File(queryFilePath), IcebergQuery.class);

        // load .dz file
        String dataSourceName = query.getDataSource();
        CoolModel coolModel = new CoolModel(dzFilePath);
        coolModel.reload(dataSourceName);

        // execute query
        QueryResult result = wrapResult(coolModel.getCube(dataSourceName), query);
        System.out.println(result.toString());
    }

    /**
     * execute query
     *
     * @param cube the cube that stores the data we need
     * @param query the cohort query needed to process
     * @return the result of the query
     */

    public static List<BaseResult> executeQuery(CubeRS cube, IcebergQuery query) throws Exception{
        long beg;
        long end;
        List<CubletRS> cublets = cube.getCublets();
        TableSchema tableSchema = cube.getTableSchema();
        List<BaseResult> results = new ArrayList<>();

        beg = System.currentTimeMillis();
        IcebergSelection selection = new IcebergSelection();
        selection.init(tableSchema, query);
        end = System.currentTimeMillis();
        //System.out.println("selection init elapsed: " + (end - beg));
        for (CubletRS cubletRS : cublets) {
            MetaChunkRS metaChunk = cubletRS.getMetaChunk();
            beg = System.currentTimeMillis();
            selection.process(metaChunk);
            end = System.currentTimeMillis();
            //System.out.println("selection process meta chunk elapsed: " + (end - beg));
            if (selection.isbActivateCublet()) {
                List<ChunkRS> datachunks = cubletRS.getDataChunks();
                for (ChunkRS dataChunk : datachunks) {
                    beg = System.currentTimeMillis();
                    Map<String, BitSet> map = selection.process(dataChunk);
                    end = System.currentTimeMillis();
                    //System.out.println("selection process data chunk elapsed: " + (end - beg));
                    if (map == null) {
                        continue;
                    }
                    for (Map.Entry<String, BitSet> entry : map.entrySet()) {
                        String timeRange = entry.getKey();
                        BitSet bs = entry.getValue();
                        beg = System.currentTimeMillis();
                        IcebergAggregation icebergAggregation = new IcebergAggregation();
                        icebergAggregation.init(bs, query.getGroupFields(), metaChunk, dataChunk, timeRange);
                        end = System.currentTimeMillis();
                        //System.out.println("init aggregation elapsed: " + (end - beg));
                        for (Aggregation aggregation : query.getAggregations()) {
                            beg = System.currentTimeMillis();
                            List<BaseResult> res = icebergAggregation.process(aggregation);
                            end = System.currentTimeMillis();
                            //System.out.println("aggregation process elapsed: " + (end - beg));
                            results.addAll(res);
                        }
                    }
                }
            }
        }
        results = BaseResult.merge(results);
        return results;
    }

    public static QueryResult wrapResult(CubeRS cube, IcebergQuery query) {
        try {
            List<BaseResult> results = executeQuery(cube, query);
            return QueryResult.ok(results);
        } catch (Exception e) {
            e.printStackTrace();
            return QueryResult.error("something wrong");
        }
    }




}
