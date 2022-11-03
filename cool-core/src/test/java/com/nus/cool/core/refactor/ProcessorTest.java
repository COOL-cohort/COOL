package com.nus.cool.core.refactor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.refactor.CohortProcessor;
import com.nus.cool.core.cohort.refactor.CohortQueryLayout;
import com.nus.cool.core.cohort.refactor.storage.CohortRet;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.functionality.CsvLoaderTest;
import com.nus.cool.model.CoolModel;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
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

  private final String queryName = "query.json";
  private final String resultName = "query_result.json";

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
<<<<<<< HEAD

  @Test(dataProvider = "ProcessQueryDP", dependsOnMethods = {
      "com.nus.cool.functionality.CsvLoaderTest.csvLoaderUnitTest"})
  public void processQueryAndValidResult(String queryDir) throws IOException {
=======
  @Test(dataProvider = "ProcessQueryDP", dependsOnMethods = {"CsvLoaderUnitTest"})
  public void ProcessQueryAndValidResult(String queryDir) throws IOException {
>>>>>>> 02ba6f0 (Solve conflicts)
    String queryPath = Paths.get(queryDir, this.queryName).toString();
    CohortQueryLayout layout = CohortQueryLayout.readFromJson(queryPath);
    CohortProcessor cohortProcessor = new CohortProcessor(layout);

<<<<<<< HEAD
    // get current dir path
    File currentVersion = this.coolModel.loadLatestVersion(cohortProcessor.getDataSource());
    // cohortProcessor.readExistingCohort("../CubeRepo/health_raw/v00000001");
    CohortRet ret = cohortProcessor.process(cube);
    cohortProcessor.persistCohort(currentVersion.toString());
=======
    // start a new cool model and reload the cube
    this.coolModel = new CoolModel(this.cubeRepo);
    coolModel.reload(cohortProcessor.getDataSource());
    CubeRS cube = coolModel.getCube(cohortProcessor.getDataSource());
>>>>>>> 02ba6f0 (Solve conflicts)

    // get current dir path
    File currentVersion = this.coolModel.loadLatestVersion(cohortProcessor.getDataSource());
//    cohortProcessor.readExistingCohort("../CubeRepo/health_raw/v00000012");
    CohortRet ret = cohortProcessor.process(cube);
    cohortProcessor.persistCohort(currentVersion.toString());

    String queryResultPath = Paths.get(queryDir, this.resultName).toString();
    ObjectMapper mapper = new ObjectMapper();
    // HashMap<String, List<Integer>> cohortData = mapper.readValue(new
    // File(queryResultPath), HashMap.class);
    HashMap<String, List<Integer>> cohortData = mapper.readValue(new File(queryResultPath),
        new TypeReference<HashMap<String, List<Integer>>>() {
        });
    // check the result
    // System.out.println(ret.toString());
    // validate the cohortName
    Assert.assertEquals(ret.getCohortList().size(), cohortData.size());

    // System.out.println(ret.getCohortList());
    for (String cohortName : ret.getCohortList()) {
      Assert.assertTrue(cohortData.containsKey(cohortName));
      // System.out.printf("True Result %s\n", cohortData.get(cohortName).toString());
      // System.out.printf("Get Result %s\n", ret.getValuesByCohort(cohortName));
      Assert.assertEquals(cohortData.get(cohortName), ret.getValuesByCohort(cohortName));
      System.out.println(ret.getCohortList());
    }
  }

<<<<<<< HEAD
  /**
   * Data provider.
   */
=======
>>>>>>> 02ba6f0 (Solve conflicts)
  @DataProvider(name = "ProcessQueryDP")
  public Object[][] queryDirDataProvider() {
    return new Object[][] {{"../datasets/health_raw/sample_query_distinctcount"},
        {"../datasets/ecommerce_query/sample_query"}, {"../datasets/health_raw/sample_query_count"},
        {"../datasets/health_raw/sample_query_average"},
        {"../datasets/health_raw/sample_query_max"}, {"../datasets/health_raw/sample_query_min"},
        {"../datasets/health_raw/sample_query_sum"},
        {"../datasets/fraud_case/sample_query_login_count"},
<<<<<<< HEAD
        {"../datasets/health/sample_query_distinctcount"}};
  }
}
=======
        {"../datasets/health/sample_query_distinctcount"},};
  }

}
>>>>>>> 02ba6f0 (Solve conflicts)
