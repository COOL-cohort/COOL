package com.nus.cool.functionality;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.ExtendedCohortQuery;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.model.CoolModel;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
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
      "com.nus.cool.functionality.CsvLoaderTest.CsvLoaderUnitTest"}, enabled = false)
  public void CohortSelectionUnitTest(String datasetPath, String queryPath,
                                      List<Integer> selectionGlobalIDs) throws IOException {

    ObjectMapper mapper = new ObjectMapper();
    ExtendedCohortQuery query = mapper.readValue(new File(queryPath), ExtendedCohortQuery.class);

    String inputSource = query.getDataSource();
    CoolModel coolModel = new CoolModel(datasetPath);
    coolModel.reload(inputSource);

    CubeRS cube = coolModel.getCube(query.getDataSource());

    List<Integer> cohortResults = coolModel.cohortEngine.selectCohortUsers(cube, null, query);
    Assert.assertEquals(selectionGlobalIDs, cohortResults);
    List<String> userIDs = coolModel.cohortEngine.listCohortUsers(cube, cohortResults);
//    Assert.assertEquals(userIDs, selectionActualIDs);

    String outputCohort = query.getOutputCohort();
    File cohortRoot = new File(coolModel.getCubeStorePath(inputSource), "cohort");
    if (!cohortRoot.exists()) {
      cohortRoot.mkdir();
      logger.info("[*] Cohort Fold " + cohortRoot.getName() + " is created.");
    }
    File cohortFile = new File(cohortRoot, outputCohort);
    if (cohortFile.exists()) {
      cohortFile.delete();
      logger.info("[*] Cohort " + outputCohort + " exists and is deleted!");
    }

    coolModel.cohortEngine.createCohort(query, cohortResults, cohortRoot);
    logger.info("[*] Cohort results are stored into " + cohortRoot.getAbsolutePath());
    coolModel.close();
  }

  /**
   * Data provider.
   */
  @DataProvider(name = "CohortSelectionTestDP")
  public Object[][] cohortSelectionTestDPArgObjects() {
    return new Object[][] {
        {Paths.get(System.getProperty("user.dir"), "..", "CubeRepo/TestCube").toString(),
            Paths.get(System.getProperty("user.dir"), "..",
                "datasets/health/sample_query_distinctcount", "query.json").toString(),
            // output global IDs
            Arrays.asList(1, 1, 0, 0, 0, 0, 0, 0, 0)}};
  }
}
