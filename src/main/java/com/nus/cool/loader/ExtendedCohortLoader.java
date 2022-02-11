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
import com.google.common.collect.Maps;
import com.nus.cool.core.cohort.CohortQuery;
import com.nus.cool.core.cohort.ExtendedCohortQuery;
import com.nus.cool.core.cohort.QueryResult;
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

        if(!query.isValid())
            throw new IOException("Wrong cohort query");

        System.out.println("Get cube:" + coolModel.getCube(query.getDataSource()));

        long start = System.currentTimeMillis();

        System.out.println("Input Cohort: " + query.getInputCohort());
        String inputCohorts = query.getInputCohort();
        if (inputCohorts!=null){
            coolModel.loadCohorts(inputCohorts, testPath+"/"+dataPath);
        }

        InputVector userVector = coolModel.getCohortUsers(query.getInputCohort());

        System.out.println(userVector);

//        QueryResult result = selectCohortUsers(coolModel.getCube(query.getDataSource()),null, query);
//
//        System.out.println(" result for query0 is  " + result);

        long end = System.currentTimeMillis();
        System.out.println("Exe time: " + (end - start));


    }

}
