package com.nus.cool.core.refactor;

import com.nus.cool.core.cohort.refactor.CohortProcessor;
import com.nus.cool.core.cohort.refactor.CohortQueryLayout;
import com.nus.cool.core.cohort.refactor.storage.CohortRet;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.functionality.CsvLoaderTest;
import com.nus.cool.model.CoolModel;
import java.io.File;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Testing cohort processor.
 */
public class ProcessorTest extends CsvLoaderTest {
  static final Logger logger = LoggerFactory.getLogger(ProcessorTest.class);
  private final String cubeRepo = "../CubeRepo/TestCube";
  private CoolModel coolModel;

  @BeforeTest
  public void setUp() {
    logger.info("Start UnitTest " + ProcessorTest.class.getSimpleName());
  }

  public void tearDown() {
    logger.info(String.format("Tear down UnitTest %s\n", ProcessorTest.class.getSimpleName()));
  }

  /**
   * Testing cohort query.
   */

  @Test(dataProvider = "ProcessQueryDP", dependsOnMethods = {
      "com.nus.cool.functionality.CsvLoaderTest.csvLoaderUnitTest"})
  public void processQueryAndValidResult(String queryPath, String queryResultPath)
      throws IOException {
    CohortQueryLayout layout = CohortQueryLayout.readFromJson(queryPath);
    CohortProcessor cohortProcessor = new CohortProcessor(layout);

    // start a new cool model and reload the cube
    this.coolModel = new CoolModel(this.cubeRepo);
    coolModel.reload(cohortProcessor.getDataSource());
    CubeRS cube = coolModel.getCube(cohortProcessor.getDataSource());
    File currentVersion = this.coolModel.getCubeStorePath(cohortProcessor.getDataSource());

    // load input cohort
    if (cohortProcessor.getInputCohort() != null) {
      File cohortFile = new File(currentVersion, "cohort/" + cohortProcessor.getInputCohort());
      if (cohortFile.exists()) {
        cohortProcessor.readOneCohort(cohortFile);
      }
    }

    // get current dir path
    CohortRet ret = cohortProcessor.process(cube);
    String cohortStoragePath = cohortProcessor.persistCohort(currentVersion.toString());
    cohortProcessor.readQueryCohorts(cohortStoragePath);
    Assert.assertTrue(cohortProcessor.getPreviousCohortUsers().size() > 0);
  }

  /**
   * Data provider.
   */
  @DataProvider(name = "ProcessQueryDP")
  public Object[][] queryDirDataProvider() {
    return new Object[][] {
        // heath_rew
        {"../datasets/health_raw/sample_query_distinctcount/query.json",
            "../datasets/health_raw/sample_query_distinctcount/query_result.json"},
    };
  }
}