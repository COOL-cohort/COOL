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

import com.nus.cool.core.cohort.CohortProcessor;
import com.nus.cool.core.cohort.CohortQueryLayout;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.model.CoolModel;
import java.io.File;
import java.io.IOException;

/**
 * CohortLoader is a higher level abstraction for cohort query.
 * With CohortLoader, you can easily get the query result
 */
public class CohortLoader {

  /**
   * Local cohort results for cool.
   *
   * @param args [0] the output data dir (eg, dir of .dz file)
   *             [1] path to the cohort file.
   *             e.g., datasets/health_raw/sample_query_cohort_loader/query.json
   * @throws IOException when CoolModel throw IOException
   */
  public static void main(String[] args) throws IOException {
    String cubeRepo = args[0];
    String queryPath = args[1];

    CohortQueryLayout layout = CohortQueryLayout.readFromJson(queryPath);
    CohortProcessor cohortProcessor = new CohortProcessor(layout);

    // start a new cool model and reload the cube
    CoolModel coolModel = new CoolModel(cubeRepo);
    coolModel.reload(cohortProcessor.getDataSource());
    CubeRS cube = coolModel.getCube(cohortProcessor.getDataSource());
    File currentVersion = coolModel.getCubeStorePath(cohortProcessor.getDataSource());

    // load cohort
    if (cohortProcessor.getInputCohort() != null) {
      File cohortFile = new File(currentVersion, cohortProcessor.getInputCohort());
      if (cohortFile.exists()) {
        cohortProcessor.readOneCohort(cohortFile);
      }
    }

    coolModel.close();
  }
}
