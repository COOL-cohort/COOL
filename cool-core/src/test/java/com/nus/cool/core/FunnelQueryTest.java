package com.nus.cool.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.FunnelQueryLayout;
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
 * Testing funnel query.
 */

public class FunnelQueryTest {
  static final Logger logger = LoggerFactory.getLogger(FunnelQueryTest.class);

  @BeforeTest
  public void setUp() {
    logger.info("Start UnitTest " + FunnelQueryTest.class.getSimpleName());
  }

  @AfterTest
  public void tearDown() {
    logger.info(String.format("Tear down UnitTest %s\n", FunnelQueryTest.class.getSimpleName()));
  }

  @Test(dataProvider = "FunnelQueryTestDP")
  public void funnelQueryUnitTest(String queryPath)
      throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    FunnelQueryLayout query = mapper.readValue(new File(queryPath), FunnelQueryLayout.class);
    Assert.assertTrue(query.isValid());
  }

  /**
   * Data provider.
   */
  @DataProvider(name = "FunnelQueryTestDP")
  public Object[][] funnelQueryTestDPArgObjects() {
    return new Object[][] {
        {"../datasets/sogamo/sample_funnel_analysis/query.json"}};
  }
}
