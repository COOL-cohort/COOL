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
import com.nus.cool.core.cohort.ExtendedCohortQuery;
import com.nus.cool.model.CoolModel;

import java.io.File;
import java.io.IOException;

public class CohortCreator {

    public static void main(String[] args) throws IOException {
        String datasetPath = args[0];
        String appPath = args[1];
        String queryPath = args[2];

        CoolModel coolModel = new CoolModel(datasetPath);
        coolModel.reload(appPath);

        // cohort query from json
        ObjectMapper mapper = new ObjectMapper();
        ExtendedCohortQuery query = mapper.readValue(new File(queryPath), ExtendedCohortQuery.class);

        System.out.println(" ------  checking query info ------ ");
        System.out.println(query);
        System.out.println(" ------  checking query info done ------ ");

        String outputCohort = query.getOutputCohort();
        File cohortRoot =  new File(coolModel.getCubeStorePath(appPath), "cohort");
        if(!cohortRoot.exists()){
            cohortRoot.mkdir();
            System.out.println("[*] Cohort Fold " + cohortRoot.getName() + " is created.");
        }

    }




}
