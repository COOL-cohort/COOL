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
import com.nus.cool.core.cohort.storage.CohortRSStr;
import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.util.reader.CoolTupleReader;
import com.nus.cool.core.util.writer.CsvDataWriter;
import com.nus.cool.model.CoolModel;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Cohort exploration operation.
 */
public class CohortExploration {

  /**
   * export all records of a cohort into a csv file.
   *
   * @param cubeRepo cubeRepo path
   * @param cubeName cube name
   * @param cohortName cohort name
   * @param output output file path
   */

  public static void exportCohort(String cubeRepo, String cubeName, String cohortName,
      String output) throws IOException {
    CoolModel coolModel = new CoolModel(cubeRepo);
    coolModel.reload(cubeName);

    // load cohort
    CohortRSStr crs = new CohortRSStr(StandardCharsets.UTF_8);
    File cohortFile = new File(coolModel.getLatestVersion(cubeName), "cohort/" + cohortName);
    crs.readFrom(Files.map(cohortFile));
    List<String> userVector = crs.getUsers();
    Set<String> userSet = new HashSet<>(userVector);
    System.out.println("Cohort size: " + userVector.size());

    CubeRS inputCube = coolModel.getCube(cubeName);
    CoolTupleReader reader = new CoolTupleReader(inputCube, userSet);
    CsvDataWriter writer = new CsvDataWriter(output, inputCube.getSchema());
    writer.initialize();

    // export cohort
    while (reader.hasNext()) {
      FieldValue[] tuple = (FieldValue[]) reader.next();
      writer.add(tuple);
    }
    reader.close();
    writer.close();
    coolModel.close();
  }

  /**
   * run the cohort exploration function that export all records of a cohort into a csv file.
   *
   * @param args [0]: cubeRepo [1]: cubeName [2]: cohortName [3]: output file path
   */
  public static void main(String[] args) {
    String cubeRepo = args[0];
    String cubeName = args[1];
    String cohortName = args[2];
    String output = args[3];
    try {
      exportCohort(cubeRepo, cubeName, cohortName, output);

      System.out.println("[*] Cohort records are exported into " + output);
    } catch (IOException e) {
      System.out.println(e);
    }
  }
}
