package com.nus.cool.core;

import com.nus.cool.core.cohort.CohortProcessor;
import com.nus.cool.core.cohort.CohortQueryLayout;
import com.nus.cool.core.cohort.OlapProcessor;
import com.nus.cool.core.cohort.OlapQueryLayout;
import com.nus.cool.core.cohort.aggregate.AggregateType;
import com.nus.cool.core.cohort.storage.CohortRet;
import com.nus.cool.core.cohort.storage.OlapRet;
import com.nus.cool.core.cohort.storage.RetUnit;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.functionality.CsvLoaderTest;
import com.nus.cool.model.CoolModel;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
            "../datasets/health_raw/sample_query_distinctcount/query_result.json"},};
  }

  /**
   * Test OLAP query.
   */
  @Test(dataProvider = "ProcessQueryAP", dependsOnMethods = {
      "com.nus.cool.functionality.CsvLoaderTest.csvLoaderUnitTest"})
  public void ProcessQueryAndValidResultAP(String queryDir) throws Exception {
    String queryName = "query.json";
    String resultName = "query_result.json";
    String queryPath = Paths.get(queryDir, queryName).toString();
    OlapQueryLayout layout = OlapQueryLayout.readFromJson(queryPath);
    String dataSource = layout.getDataSource();
    OlapProcessor olapProcessor = new OlapProcessor(layout);

    // start a new cool model and reload the cube
    this.coolModel = new CoolModel(this.cubeRepo);
    coolModel.reload(dataSource);
    CubeRS cube = coolModel.getCube(dataSource);
    List<OlapRet> ret = olapProcessor.process(cube);
    // System.out.println(ret);

    // verification:
    if (queryDir.equals("../datasets/olap-tpch")) {
      ArrayList<OlapRet> result = generateResultForTPCH();
      for (int i = 0; i < ret.size(); i++) {
        Assert.assertEquals(result.get(i), ret.get(i));
      }
    }

    if (queryDir.equals("../datasets/ecommerce/queries")) {
      ArrayList<OlapRet> result = generateResultForEcommerce();
      for (int i = 0; i < ret.size(); i++) {
        Assert.assertEquals(result.get(i), ret.get(i));
      }
    }
  }

  @DataProvider(name = "ProcessQueryAP")
  public Object[][] queryDirDataProviderAP() {
    return new Object[][] {
        {"../datasets/olap-tpch"},
        {"../datasets/ecommerce/queries"}};
  }

  private ArrayList<OlapRet> generateResultForTPCH() {
    Map<String, Map<AggregateType, RetUnit>> resultMap = new HashMap<>();
    resultMap.put("RUSSIA|EUROPE", new HashMap<AggregateType, RetUnit>() {{
      put(AggregateType.COUNT, new RetUnit(2, 0));
      put(AggregateType.SUM, new RetUnit(312855, 0));
    }});

    resultMap.put("GERMANY|EUROPE", new HashMap<AggregateType, RetUnit>() {{
      put(AggregateType.COUNT, new RetUnit(1, 0));
      put(AggregateType.SUM, new RetUnit(4820, 0));
    }});

    resultMap.put("ROMANIA|EUROPE", new HashMap<AggregateType, RetUnit>() {{
      put(AggregateType.COUNT, new RetUnit(2, 0));
      put(AggregateType.SUM, new RetUnit(190137, 0));
    }});

    resultMap.put("UNITED KINGDOM|EUROPE", new HashMap<AggregateType, RetUnit>() {{
      put(AggregateType.COUNT, new RetUnit(1, 0));
      put(AggregateType.SUM, new RetUnit(33248, 0));
    }});

    ArrayList<OlapRet> results = new ArrayList<>();

    for (Map.Entry<String, Map<AggregateType, RetUnit>> entry : resultMap.entrySet()) {
      String groupName = entry.getKey();
      Map<AggregateType, RetUnit> groupValue = entry.getValue();

      // assign new result
      OlapRet newEle = new OlapRet();
      newEle.setTimeRange(null);
      newEle.setKey(groupName);
      newEle.setFieldName("O_TOTALPRICE");
      newEle.initAggregator(groupValue);
      results.add(newEle);
    }
    return results;
  }

  private ArrayList<OlapRet> generateResultForEcommerce() {
    Map<String, Map<AggregateType, RetUnit>> resultMap = new HashMap<>();
    resultMap.put("2021-01", new HashMap<AggregateType, RetUnit>() {{
      put(AggregateType.DISTINCT, new RetUnit(235, 0));
    }});
    ArrayList<OlapRet> results = new ArrayList<>();

    for (Map.Entry<String, Map<AggregateType, RetUnit>> entry : resultMap.entrySet()) {
      String groupName = entry.getKey();
      Map<AggregateType, RetUnit> groupValue = entry.getValue();

      // assign new result
      OlapRet newEle = new OlapRet();
      newEle.setTimeRange(null);
      newEle.setKey(groupName);
      newEle.setFieldName("Product_ID");
      newEle.initAggregator(groupValue);
      results.add(newEle);
    }
    return results;
  }
}