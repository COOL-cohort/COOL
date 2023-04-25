package com.nus.cool.functionality;

import static com.nus.cool.functionality.CohortAnalysis.performCohortAnalysis;
import static com.nus.cool.functionality.OLAPAnalysis.performOLAPAnalysis;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.storage.CohortRet;
import com.nus.cool.core.cohort.storage.OLAPRet;
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
public class OLAPAnalysisTest {

  @Test(dataProvider = "OLAPAnalysisTestDPArgObjects", dependsOnMethods = {
      "com.nus.cool.functionality.CsvLoaderTest.csvLoaderUnitTest"})
  public void analysisUnitTest(String cubeRepo, String queryPath) throws IOException {
    List<OLAPRet>  ret = performOLAPAnalysis(cubeRepo, queryPath);
    System.out.println(ret);
  }

  /**
   * Data provider for cohort analysis without input cohort.
   */
  @DataProvider(name = "OLAPAnalysisTestDPArgObjects")
  public Object[][] dataProvider() {
    String cubeRepo = Paths.get(System.getProperty("user.dir"), "..",
        "CubeRepo/TestCube").toString();
    return new Object[][] {
        // ecommerce
        {cubeRepo,
         "../datasets/olap-tpch/query.json"},
        // heath_raw
        {cubeRepo,
         "../datasets/ecommerce/queries/query.json"},
    };
  }

}
