///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//
//package com.nus.cool.functionality;
//
//import com.nus.cool.core.io.readstore.CubeRS;
//import com.nus.cool.core.io.storevector.InputVector;
//import com.nus.cool.core.util.writer.CliDataWriter;
//import com.nus.cool.core.util.writer.DataWriter;
//import com.nus.cool.model.CoolModel;
//import java.io.IOException;
//
///**
// * Cohort exploration operation.
// */
//public class CohortExploration {
//
//  /**
//   * perform cohort exploration.
//   */
//  public static void main(String[] args) {
//    String datasetPath = args[0];
//    String cubeName = args[1];
//    String cohortName = args[2];
//    try {
//      CoolModel coolModel = new CoolModel(datasetPath);
//
//      // load cube
//      coolModel.reload(cubeName);
//      CubeRS inputCube = coolModel.getCube(cubeName);
//
//      // load cohort
//      coolModel.loadCohorts(cohortName, cubeName);
//      InputVector userVector = coolModel.getCohortUsers(cohortName);
//
//      // export cohort
//      DataWriter writer = new CliDataWriter();
//      coolModel.cohortEngine.exportCohort(inputCube, userVector, writer);
//
//      coolModel.close();
//    } catch (IOException e) {
//      System.out.println(e);
//    }
//  }
//}
