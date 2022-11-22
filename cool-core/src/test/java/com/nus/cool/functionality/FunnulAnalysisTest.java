package com.nus.cool.functionality;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.refactor.CohortProcessor;
import com.nus.cool.core.cohort.refactor.CohortQueryLayout;
import com.nus.cool.core.cohort.refactor.funnelQuery;
import com.nus.cool.core.cohort.refactor.storage.CohortRet;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.model.CoolModel;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
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
public class FunnulAnalysisTest {
  private final String cubeRepo = "../CubeRepo/TestCube";
  private final String queryName = "query.json";
  static final Logger logger = LoggerFactory.getLogger(FunnulAnalysisTest.class);
  private CoolModel coolModel;

  @BeforeTest
  public void setUp() {
    logger.info("Start UnitTest " + FunnulAnalysisTest.class.getSimpleName());
  }

  @AfterTest
  public void tearDown() {
    logger.info(String.format("Tear down UnitTest %s\n", FunnulAnalysisTest.class.getSimpleName()));
  }

  @Test(dataProvider = "FunnelAnalysisTestDP", dependsOnMethods = {
      "com.nus.cool.functionality.CsvLoaderTest.csvLoaderUnitTest"})
  public void funnelAnalysisUnitTest(String queryDir, int[] out)
      throws IOException {
    String queryPath = Paths.get(queryDir, this.queryName).toString();
    ObjectMapper mapper = new ObjectMapper();
    funnelQuery query = mapper.readValue(new File(queryPath), funnelQuery.class);
    this.coolModel = new CoolModel(this.cubeRepo);
    String dataSource=query.getDataSource();
    coolModel.reload(dataSource);
    CubeRS cube = coolModel.getCube(dataSource);
    int[] ret = coolModel.cohortEngine.funnelAnalysis(cube, query);
    Assert.assertEquals(ret, out);
    System.out.println(1);
  }

  /**
   * Data provider.
   */
  @DataProvider(name = "FunnelAnalysisTestDP")
  public Object[][] funnelAnalysisTestDPArgObjects() {
    int[] out = {5, 5, 4, 4};
    return new Object[][] {{"../datasets/sogamo/sample_funnel_analysis", out}};
  }
}