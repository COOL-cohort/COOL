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
import com.nus.cool.core.cohort.funnel.FunnelQuery;

import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.model.CoolModel;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class FunnelAnalysis {

    public static void main(String[] args) {
        String datasetPath = args[0];
        String queryPath = args[1];

        try {
            ObjectMapper mapper = new ObjectMapper();
            FunnelQuery query = mapper.readValue(new File(queryPath), FunnelQuery.class);

            if (!query.isValid())
                throw new IOException("[x] Invalid funnel query.");

            String inputSource = query.getDataSource();
            CoolModel coolModel = new CoolModel(datasetPath);
            coolModel.reload(inputSource);

            String inputCohort = query.getInputCohort();
            if (inputCohort != null) {
                coolModel.loadCohorts(inputCohort, inputSource);
                System.out.println("Input cohort: " + inputCohort);
            }

            InputVector userVector = coolModel.getCohortUsers(query.getInputCohort());

            int[] result = coolModel.cohortEngine.performFunnelQuery(coolModel.getCube(inputSource),userVector,query);
            System.out.println(Arrays.toString(result));
        } catch (IOException e) {
            System.out.println(e);
        }
    }
}
