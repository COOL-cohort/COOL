package com.nus.cool.functionality;

import java.io.IOException;

import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.util.writer.CliDataWriter;
import com.nus.cool.core.util.writer.DataWriter;
import com.nus.cool.model.CoolModel;

public class CohortExploration {
  public static void main(String[] args) {
    String datasetPath = args[0];
    String cubeName = args[1];
    String cohortName = args[2];
    try {
      CoolModel coolModel = new CoolModel(datasetPath);

      // load cube
      coolModel.reload(cubeName);
      CubeRS inputCube = coolModel.getCube(cubeName);

      // load cohort
      coolModel.loadCohorts(cohortName, cubeName);
      InputVector userVector = coolModel.getCohortUsers(cohortName);

      // export cohort
      DataWriter writer = new CliDataWriter();
      coolModel.cohortEngine.exportCohort(inputCube, userVector, writer);

      coolModel.close();
    } catch (IOException e) {
      System.out.println(e);
    }
  }
}
