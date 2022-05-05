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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.iceberg.aggregator.AggregatorFactory;
import com.nus.cool.core.iceberg.query.*;
import com.nus.cool.core.iceberg.result.BaseResult;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.readstore.CubletRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.schema.TableSchema;

import java.io.IOException;
import java.util.*;

public class CoolOlapEngine {

    /**
     * execute iceberg query
     *
     * @param cube the cube that stores the data we need
     * @param query the cohort query needed to process
     * @return the result of the query
     */
    public List<BaseResult> performOlapQuery(CubeRS cube, IcebergQuery query) throws Exception{
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

    public static void profiling(List<BaseResult> results) {

        HashMap<String, Float> profilingCount = new HashMap<>();
        HashMap<String, Long> profilingSum = new HashMap<>();

        Float totalCount = (float) 0;
        Float totalSum = (float) 0;
        for (BaseResult res: results){
            String k = res.getKey();
            profilingCount.put(k, res.getAggregatorResult().getCount());
            profilingSum.put(k, res.getAggregatorResult().getSum());
            totalCount += res.getAggregatorResult().getCount();;
            totalSum += res.getAggregatorResult().getSum();
        }

        for (String key : profilingCount.keySet()){
            Float value = profilingCount.get(key)/totalCount;
            profilingCount.put(key, value);
            value = value*100;
            System.out.println("Key = "+ key +", percentage of matched records = " + value.toString().substring(0,4)+"%");
        }

        for (String key : profilingSum.keySet()){
            long sumValue = profilingSum.get(key);
            float value = sumValue/totalSum;
            value = value*100;
            System.out.println("Key = "+ key +", percentage of aggregation = " + Float.toString(value).substring(0,4)+"%");
        }
    }

    public static final String jsonString = "{\n" +
            "  \"dataSource\": \"default\",\n" +
            "  \"selection\": {\n" +
            "    \"type\": \"filter\",\n" +
            "    \"dimension\": \"default\",\n" +
            "    \"values\": [ \"default\" ],\n" +
            "    \"fields\":[]\n" +
            "  },\n" +
            "  \"aggregations\":[\n" +
            "    {\"fieldName\":\"default\",\n" +
            "      \"operators\":[\"COUNT\"]}]\n" +
            "}";

    public IcebergQuery generateQuery(String operation, String dataSourceName) throws IOException {

        String[] parsedOpe = operation.split(", ");

        if (!parsedOpe[0].equals("select")){
            System.out.println(Arrays.toString(parsedOpe) +", operation Not supported");
            return null;
        }

        String filed = parsedOpe[1];
        String value = parsedOpe[2];

        List<String> values = new ArrayList<>();
        values.add(value);

        // init query
        ObjectMapper mapper = new ObjectMapper();
        IcebergQuery query = mapper.readValue(jsonString, IcebergQuery.class);

        // update query
        query.setDataSource(dataSourceName);

        SelectionQuery sq = query.getSelection();
        sq.setType(SelectionQuery.SelectionType.filter);
        sq.setDimension(filed);
        sq.setValues(values);

        query.setSelection(sq);

        List<Aggregation> aggregations = new ArrayList<>();
        Aggregation agg = new Aggregation();

        List<AggregatorFactory.AggregatorType> opt = new ArrayList<>();
        opt.add(AggregatorFactory.AggregatorType.COUNT);

        agg.setOperators(opt);
        agg.setFieldName(filed);

        aggregations.add(agg);

        query.setAggregations(aggregations);

        return query;
    }

}
