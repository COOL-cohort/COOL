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
import com.nus.cool.core.iceberg.query.IcebergQuery;
import com.nus.cool.core.iceberg.result.BaseResult;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.model.CoolModel;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static com.nus.cool.functionality.IcebergLoader.executeQuery;


public class CohortProfiling {

    /**
     * IcebergLoader model for cool
     *
     * @param args [0] the output data dir (eg, dir of .dz file)
     *        args [1] query's path, eg olap-tpch/query0.json
     * @throws IOException
     */
    public static void main(String[] args) throws Exception {
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
        CubeRS cube = coolModel.getCube(dataSourceName);
        Profiling(cube, query);
    }

    public static void Profiling(CubeRS cube, IcebergQuery query) throws Exception {
        // execute query
        List<BaseResult> results = executeQuery(cube, query);

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


}
