package com.nus.cool.functionality;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.refactor.FunnelProcessor;
import com.nus.cool.core.cohort.refactor.FunnelQuery;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.model.CoolModel;
import java.io.File;
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
  private CoolModel coolModel;

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
    ObjectMapper mapper = new ObjectMapper();
    FunnelQuery query = mapper.readValue(new File(queryPath), FunnelQuery.class);
    Assert.assertTrue(query.isValid());
    this.coolModel = new CoolModel(cubeRepo);
    String dataSource = query.getDataSource();
    coolModel.reload(dataSource);
    FunnelProcessor funnelProcessor = new FunnelProcessor(query);
    CubeRS cube = coolModel.getCube(dataSource);
    int[] ret = funnelProcessor.process(cube);
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