package com.nus.cool.functionality;

import com.google.common.io.Files;
import com.nus.cool.core.cohort.storage.CohortRSStr;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Testing cohort selection.
 */
public class CohortSelectionTest {
  static final Logger logger = LoggerFactory.getLogger(CohortSelectionTest.class);

  @BeforeTest
  public void setUp() {
    logger.info("Start UnitTest " + CohortSelectionTest.class.getSimpleName());
  }

  @AfterTest
  public void tearDown() {
    logger.info(
        String.format("Tear Down UnitTest %s\n", CohortSelectionTest.class.getSimpleName()));
  }

  @Test(dataProvider = "CohortSelectionTestDP", dependsOnMethods = {
      "com.nus.cool.functionality.CsvLoaderTest.csvLoaderUnitTest"})
  public void cohortSelectionUnitTest(String cubeRepo, String queryPath, int cohortSize)
      throws IOException {

    String cohortStoragePath = CohortSelection.performCohortSelection(cubeRepo, queryPath);

    File cohortResFile = new File(cohortStoragePath, "all.cohort");
    CohortRSStr crs = new CohortRSStr(StandardCharsets.UTF_8);
    crs.readFrom(Files.map(cohortResFile));
    List<String> redRes = crs.getUsers();
    Assert.assertEquals(redRes.size(), cohortSize);
  }

  /**
   * Data provider.
   */
  @DataProvider(name = "CohortSelectionTestDP")
  public Object[][] cohortSelectionTestDPArgObjects() {
    return new Object[][] {
        {Paths.get(System.getProperty("user.dir"), "..", "CubeRepo/TestCube").toString(),
            Paths.get(System.getProperty("user.dir"), "..",
                "datasets/health_raw/sample_query_selection", "query.json").toString(),
            8592,
        }};
  }
}
