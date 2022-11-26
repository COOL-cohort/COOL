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
import com.nus.cool.core.cohort.FunnelProcessor;
import com.nus.cool.core.cohort.FunnelQueryLayout;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.model.CoolModel;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;

/**
 * Funnel analysis operation.
 */
public class FunnelAnalysis {

  /**
   * API to perform funnel analysis.
   *
   * @param cubeRepo path to the cube repository
   * @param queryPath path to the query
   * @return result of funnel analysis
   * @throws IOException error
   */
  public static int[] performFunnelAnalysis(String cubeRepo, String queryPath) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    FunnelQueryLayout query = mapper.readValue(new File(queryPath), FunnelQueryLayout.class);
    if (!query.isValid()) {
      throw new IOException("[x] Invalid funnel query.");
    }

    CoolModel coolModel = new CoolModel(cubeRepo);
    String dataSource = query.getDataSource();
    coolModel.reload(dataSource);
    FunnelProcessor funnelProcessor = new FunnelProcessor(query);
    CubeRS cube = coolModel.getCube(dataSource);
    int[] ret = funnelProcessor.process(cube);
    coolModel.close();
    return ret;
  }


  /**
   * Execute funnel analysis query.
   *
   * @param args [0]: cubeRepo
   *             [1]: queryPath
   */
  public static void main(String[] args) {
    String cubeRepo = args[0];
    String queryPath = args[1];

    try {
      int[] result = performFunnelAnalysis(cubeRepo, queryPath);
      System.out.println(Arrays.toString(result));
    } catch (IOException e) {
      System.out.println(e);
    }
  }
}
