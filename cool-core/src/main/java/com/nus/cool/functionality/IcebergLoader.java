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
import java.util.List;

/**
 * IcebergLoader is a higher level abstraction for iceberg query
 * With IcebergLoader, you can easily get the query result
 */
public class IcebergLoader {

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

        // execute query
        List<BaseResult> result = coolModel.olapEngine.performOlapQuery(coolModel.getCube(dataSourceName), query);
        System.out.println(result);
    }

}
