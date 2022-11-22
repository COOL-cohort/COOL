package com.nus.cool.core.refactor;

import com.google.common.io.Files;
import com.nus.cool.core.cohort.refactor.storage.CohortRSStr;
import com.nus.cool.core.cohort.refactor.storage.CohortWSStr;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import org.testng.annotations.Test;

/**
 * Testing CohortStoreTest.
 */
public class CohortStoreTest {

  /**
   *
   * Testing cohort query.
   */
  @Test()
  public void storeCohort() throws IOException {

    System.out.println(System.getProperty("user.dir"));

    ArrayList<String> testData =
        new ArrayList<>(Arrays.asList("Userid1", "Userid2", "Userid3", "Userid4", "Userid5"));

    CohortWSStr cws = new CohortWSStr(StandardCharsets.UTF_8);

    for (String userID : testData) {
      cws.addCubletResults(userID);
    }

    // output file
    String fileName = "TestCohort";
    File cubemeta = new File(System.getProperty("user.dir"), fileName);
    DataOutputStream out = new DataOutputStream(new FileOutputStream(cubemeta));
    cws.writeTo(out);

    // read the above
    CohortRSStr crs = new CohortRSStr(StandardCharsets.UTF_8);
    crs.readFrom(Files.map(cubemeta).order(ByteOrder.nativeOrder()));
    System.out.println(crs.getUsers());
    cubemeta.delete();
  }

}
