package com.nus.cool.core;

import com.google.common.io.Files;
import com.nus.cool.core.cohort.storage.CohortRSStr;
import com.nus.cool.core.cohort.storage.CohortWSStr;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Testing CohortStoreTest.
 */
public class CohortStoreTest {

  /**
   * Testing cohort query.
   */
  @Test(dataProvider = "storeCohortDP")
  public void storeCohort(ArrayList<String> testData) throws IOException {

    CohortWSStr cws = new CohortWSStr(StandardCharsets.UTF_8);

    for (String userID : testData) {
      cws.addCubletResults(userID);
    }

    // output file
    String fileName = "TestCohort";
    File cohortResFile = new File(System.getProperty("user.dir"), fileName);
    DataOutputStream out = new DataOutputStream(new FileOutputStream(cohortResFile));
    cws.writeTo(out);

    // read the above
    CohortRSStr crs = new CohortRSStr(StandardCharsets.UTF_8);
    crs.readFrom(Files.map(cohortResFile));
    List<String> redRes = crs.getUsers();

    // assert the read the original data.
    Boolean isEqual = testData.containsAll(redRes) && redRes.containsAll(testData);
    Assert.assertEquals(isEqual, Boolean.TRUE);

    // delete the file just write.
    cohortResFile.delete();
  }

  /**
   * Data provider for storeCohortDP.
   */
  @DataProvider(name = "storeCohortDP")
  private Object[] storeCohortDataProvider() {
    ArrayList<String> testData =
        new ArrayList<>(Arrays.asList("Userid1", "Userid2", "Userid3", "Userid4", "Userid5"));

    return new Object[] {testData};
  }
}
