package com.nus.cool.functionality;

import com.nus.cool.core.cohort.storage.OLAPRet;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Testing cohort selection.
 */
public class OLAPAnalysisTest {

  @Test(dataProvider = "OLAPAnalysisTestDPArgObjects", dependsOnMethods = {
      "com.nus.cool.functionality.CsvLoaderTest.csvLoaderUnitTest"})
  public void analysisUnitTest(String cubeRepo, String queryPath) throws IOException {
    List<OLAPRet>  ret = OLAPAnalysis.performOLAPAnalysis(cubeRepo, queryPath);
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
