
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
import com.nus.cool.core.cohort.OLAPProcessor;
import com.nus.cool.core.cohort.OLAPQueryLayout;
import com.nus.cool.core.cohort.storage.CohortRet;
import com.nus.cool.core.cohort.storage.OLAPRet;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.model.CoolModel;
import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * OLAP analysis operation.
 */
public class OLAPAnalysis {
  /**
   * perform OLAP query to conduct cohort analysis.
   *
   * @param cubeRepo cube path: the path to all datasets, e.g., CubeRepo
   * @param queryPath query path: the path to the cohort query, e.g.,
   *                  datasets/ecommerce/queries/query.json
   */
  public static List<OLAPRet> performOLAPAnalysis(String cubeRepo, String queryPath)
      throws IOException {

    OLAPQueryLayout layout = OLAPQueryLayout.readFromJson(queryPath);
    OLAPProcessor olapProcessor = new OLAPProcessor(layout);

    // start a new cool model and reload the cube
    CoolModel coolModel = new CoolModel(cubeRepo);
    coolModel.reload(layout.getDataSource());
    CubeRS cube = coolModel.getCube(layout.getDataSource());
    List<OLAPRet> ret = olapProcessor.processCube(cube);
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
      List<OLAPRet> ret = performOLAPAnalysis(cubeRepo, queryPath);
      System.out.println("Result for the query is  " + ret.toString());
    } catch (IOException e) {
      System.out.println(e);
    }
  }
}
