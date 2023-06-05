
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

import com.google.common.io.Files;
import com.nus.cool.core.cohort.CohortProcessor;
import com.nus.cool.core.cohort.CohortQueryLayout;
import com.nus.cool.core.cohort.CohortWriter;
import com.nus.cool.core.cohort.storage.CohortRet;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.model.CoolModel;
import java.io.File;
import java.io.IOException;

/**
 * Cohort analysis operation.
 */
public class CohortAnalysis {
  /**
   * perform cohort query to conduct cohort analysis.
   *
   * @param cubeRepo cube path: the path to all datasets, e.g., CubeRepo
   * @param queryPath query path: the path to the cohort query, e.g.,
   *                  datasets/health_raw/sample_query_distinctcount/query.json
   */
  public static CohortRet performCohortAnalysis(String cubeRepo, String queryPath)
      throws IOException {
    CohortQueryLayout layout = CohortQueryLayout.readFromJson(queryPath);
    CohortProcessor cohortProcessor = new CohortProcessor(layout);

    // start a new cool model and reload the cube
    CoolModel coolModel = new CoolModel(cubeRepo);
    coolModel.reload(cohortProcessor.getDataSource());
    CubeRS cube = coolModel.getCube(cohortProcessor.getDataSource());
    File currentVersion = coolModel.getCubeStorePath(cohortProcessor.getDataSource());

    // load input cohort
    if (cohortProcessor.getInputCohort() != null) {
      File cohortFile = new File(currentVersion, "cohort/" + cohortProcessor.getInputCohort());
      if (cohortFile.exists()) {
        cohortProcessor.readOneCohort(cohortFile);
      }
    }

    // run analysis
    CohortRet ret = cohortProcessor.process(cube);

    // persist the results
    String outputPath = currentVersion.toString() + "/cohort/" + layout.getQueryName();
    CohortWriter.setUpOutputFolder(outputPath);
    Files.copy(new File(queryPath), new File(outputPath + "/query.json"));;
    CohortWriter.persistCohortResult(ret, outputPath);
    coolModel.close();
    return ret;
  }

  /**
   * API for cohort analysis.
   *
   * @param args [0]: cubeRepo
   *             [1]: queryPath
   */
  public static void main(String[] args) {
    String cubeRepo = args[0];
    String queryPath = args[1];

    try {
      CohortRet ret = performCohortAnalysis(cubeRepo, queryPath);
      System.out.println("Result for the query is  " + ret.toString());
    } catch (IOException e) {
      System.out.println(e);
    }
  }
}
