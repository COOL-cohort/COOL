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
import com.nus.cool.core.iceberg.aggregator.AggregatorFactory;
import com.nus.cool.core.iceberg.query.Aggregation;
import com.nus.cool.core.iceberg.query.IcebergQuery;
import com.nus.cool.core.iceberg.query.SelectionQuery;
import com.nus.cool.model.CoolModel;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.nus.cool.functionality.IcebergLoader.wrapResult;

public class RelationalAlgebra {

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

   public static void main(String[] args) throws IOException{
       // the path of dz file eg "COOL/cube"
       String dzFilePath = args[0];
       String dataSourceName = args[1];
       String operation = args[2];

       IcebergQuery query = generateQuery(operation, dataSourceName);
       if (query == null){
           return;
       }

       // load .dz file
       CoolModel coolModel = new CoolModel(dzFilePath);
       coolModel.reload(dataSourceName);

       // execute query
       QueryResult result = wrapResult(coolModel.getCube(dataSourceName), query);
       System.out.println(result.toString());
   }

   public static IcebergQuery generateQuery(String operation, String dataSourceName) throws IOException {

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
