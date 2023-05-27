package com.nus.cool.functionality;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Testing funnel analysis.
 */
public class FunnelAnalysisTest {
  static final Logger logger = LoggerFactory.getLogger(FunnelAnalysisTest.class);

  @BeforeTest
  public void setUp() {
    logger.info("Start UnitTest " + FunnelAnalysisTest.class.getSimpleName());
  }

  @AfterTest
  public void tearDown() {
    logger.info(String.format("Tear down UnitTest %s\n", FunnelAnalysisTest.class.getSimpleName()));
  }

  @Test(dataProvider = "FunnelAnalysisTestDP", dependsOnMethods = {
      "com.nus.cool.functionality.CsvLoaderTest.csvLoaderUnitTest"})
  public void funnelAnalysisUnitTest(String queryPath, String cubeRepo, int[] out)
      throws IOException {
    int[] ret = FunnelAnalysis.performFunnelAnalysis(cubeRepo, queryPath);
    Assert.assertEquals(ret, out);
  }

  /**
   * Data provider.
   */
  @DataProvider(name = "FunnelAnalysisTestDP")
  public Object[][] funnelAnalysisTestDPArgObjects() {
    int[] out = {5, 5, 4, 4};
    return new Object[][] {
        {"../datasets/sogamo/sample_funnel_analysis/query.json", "../CubeRepo/TestCube", out}};
  }
}