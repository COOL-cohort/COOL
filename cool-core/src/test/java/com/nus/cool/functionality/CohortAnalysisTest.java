package com.nus.cool.functionality;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.storage.CohortRet;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
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
public class CohortAnalysisTest {
  static final Logger logger = LoggerFactory.getLogger(CohortAnalysisTest.class);

  @BeforeTest
  public void setUp() {
    logger.info("Start UnitTest " + CohortAnalysisTest.class.getSimpleName());
  }

  @AfterTest
  public void tearDown() {
    logger.info(
        String.format("Tear Down UnitTest %s\n", CohortAnalysisTest.class.getSimpleName()));
  }

  @Test(dataProvider = "cohortAnalysisTestDP", dependsOnMethods = {
      "com.nus.cool.functionality.CsvLoaderTest.csvLoaderUnitTest"})
  public void cohortSelectionUnitTest(String cubeRepo, String queryPath, String queryResultPath)
      throws IOException {
    CohortRet ret = CohortAnalysis.performCohortAnalysis(cubeRepo, queryPath);

    // validate the results
    ObjectMapper mapper = new ObjectMapper();
    HashMap<String, List<Integer>> cohortData = mapper.readValue(new File(queryResultPath),
        new TypeReference<HashMap<String, List<Integer>>>() {
        });
    // check the result
    // validate the cohortName
    Assert.assertEquals(ret.getCohortList().size(), cohortData.size());

    // System.out.println(ret.getCohortList());
    for (String cohortName : ret.getCohortList()) {
      Assert.assertTrue(cohortData.containsKey(cohortName));
      Assert.assertEquals(ret.getValuesByCohort(cohortName), cohortData.get(cohortName));
    }
  }

  @Test(dataProvider = "cohortAnalysisWithInputCohortTestDP", dependsOnMethods = {
      "com.nus.cool.functionality.CsvLoaderTest.csvLoaderUnitTest",
      "com.nus.cool.functionality.CohortSelectionTest.cohortSelectionUnitTest"})
  public void cohortSelectionWithInputCohortUnitTest(String cubeRepo, String queryPath
                                                     ) throws IOException {
    CohortRet ret = CohortAnalysis.performCohortAnalysis(cubeRepo, queryPath);
    System.out.println(ret);
  }

  /**
   * Data provider for cohort analysis without input cohort.
   */
  @DataProvider(name = "cohortAnalysisTestDP")
  public Object[][] cohortAnalysisTestDPArgObjects() {
    String cubeRepo = Paths.get(System.getProperty("user.dir"), "..",
        "CubeRepo/TestCube").toString();
    return new Object[][] {
        // ecommerce
        {cubeRepo,
            "../datasets/ecommerce_query/sample_query/query.json",
            "../datasets/ecommerce_query/sample_query/query_result.json"},
        // heath_raw
        {cubeRepo,
            "../datasets/health_raw/sample_query_distinctcount/query.json",
            "../datasets/health_raw/sample_query_distinctcount/query_result.json"},
        {cubeRepo,
            "../datasets/health_raw/sample_query_count/query.json",
            "../datasets/health_raw/sample_query_count/query_result.json"},
        {cubeRepo,
            "../datasets/health_raw/sample_query_average/query.json",
            "../datasets/health_raw/sample_query_average/query_result.json"},
        {cubeRepo,
            "../datasets/health_raw/sample_query_max/query.json",
            "../datasets/health_raw/sample_query_max/query_result.json"},
        {cubeRepo,
            "../datasets/health_raw/sample_query_min/query.json",
            "../datasets/health_raw/sample_query_min/query_result.json"},
        {cubeRepo,
            "../datasets/health_raw/sample_query_sum/query.json",
            "../datasets/health_raw/sample_query_sum/query_result.json"},
        // fraud_case
        {cubeRepo,
            "../datasets/fraud_case/sample_query_login_count/query.json",
            "../datasets/fraud_case/sample_query_login_count/query_result.json"},
        // health
        {cubeRepo,
            "../datasets/health/sample_query_distinctcount/query.json",
            "../datasets/health/sample_query_distinctcount/query_result.json"},
        //health_raw random time
        {cubeRepo,
            "../datasets/health_raw_random_time/sample_query_distinctcount/query.json",
            "../datasets/health_raw_random_time/sample_query_distinctcount/query_result.json"},
        {cubeRepo,
            "../datasets/health_raw_random_time/sample_query_count/query.json",
            "../datasets/health_raw_random_time/sample_query_count/query_result.json"},
    };
  }

  /**
   * Data provider for cohort analysis with input cohort.
   */
  @DataProvider(name = "cohortAnalysisWithInputCohortTestDP")
  public Object[][] cohortAnalysisWithInputCohortTestDPArgObjects() {
    String cubeRepo = Paths.get(System.getProperty("user.dir"), "..",
        "CubeRepo/TestCube").toString();
    return new Object[][] {
        // heath_raw
        {cubeRepo,
            "../datasets/health_raw/sample_query_with_inputcohort/query.json"},

    };
  }
}
