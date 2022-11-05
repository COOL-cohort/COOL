package com.nus.cool.core.refactor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.refactor.CohortProcessor;
import com.nus.cool.core.cohort.refactor.CohortQueryLayout;
import com.nus.cool.core.cohort.refactor.OlapProcessor;
import com.nus.cool.core.cohort.refactor.OlapQueryLayout;
import com.nus.cool.core.cohort.refactor.aggregate.AggregateType;
import com.nus.cool.core.cohort.refactor.storage.CohortRet;
import com.nus.cool.core.cohort.refactor.storage.OlapRet;
import com.nus.cool.core.cohort.refactor.storage.RetUnit;
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

public class ProcessorTest extends CsvLoaderTest {
  static final Logger logger = LoggerFactory.getLogger(ProcessorTest.class);
  private final String cubeRepo = "../CubeRepo";
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
   * Test OLAP query.
   */
  @Test(dataProvider = "ProcessQueryDP", dependsOnMethods = {
      "CsvLoaderUnitTest"})
  public void ProcessQueryAndValidResult(String queryDir) throws IOException {
    String queryPath = Paths.get(queryDir, this.queryName).toString();
    CohortQueryLayout layout = CohortQueryLayout.readFromJson(queryPath);
    CohortProcessor cohortProcessor = new CohortProcessor(layout);

    // start a new cool model and reload the cube
    this.coolModel = new CoolModel(this.cubeRepo);
    coolModel.reload(cohortProcessor.getDataSource());
    CubeRS cube = coolModel.getCube(cohortProcessor.getDataSource());
    CohortRet ret = cohortProcessor.process(cube);

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

    }
  }

  @DataProvider(name = "ProcessQueryDP")
  public Object[][] queryDirDataProvider() {
    return new Object[][] {
        {"../datasets/health_raw/sample_query_distinctcount"},
        {"../datasets/ecommerce_query/sample_query"},
        {"../datasets/health_raw/sample_query_count"},
        {"../datasets/health_raw/sample_query_average"},
        {"../datasets/health_raw/sample_query_max"},
        {"../datasets/health_raw/sample_query_min"},
        {"../datasets/health_raw/sample_query_sum"},
        {"../datasets/fraud_case/sample_query_login_count"},
        {"../datasets/health/sample_query_distinctcount"},
    };
  }


  /**
   * Test OLAP query.
   */
  @Test(dataProvider = "ProcessQueryAP", dependsOnMethods = {
      "CsvLoaderUnitTest"})
  public void ProcessQueryAndValidResultAP(String queryDir) throws Exception {
    String queryPath = Paths.get(queryDir, this.queryName).toString();
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
    if (queryDir.equals("../datasets/olap-tpch")){
      ArrayList<OlapRet> result = generateResultForTPCH();
      for (int i = 0; i< ret.size(); i++){
        Assert.assertEquals(result.get(i), ret.get(i));
      }
    }

    if (queryDir.equals("../datasets/ecommerce/queries")){
      ArrayList<OlapRet> result = generateResultForEcommerce();
      for (int i = 0; i< ret.size(); i++){
        Assert.assertEquals(result.get(i), ret.get(i));
      }
    }
  }

  @DataProvider(name = "ProcessQueryAP")
  public Object[][] queryDirDataProviderAP() {
    return new Object[][] {
        {"../datasets/olap-tpch"},
        {"../datasets/ecommerce/queries"},
    };
  }


  private ArrayList<OlapRet> generateResultForTPCH(){
    Map<String, Map<AggregateType, RetUnit> > resultMap = new HashMap<>();
    resultMap.put("RUSSIA|EUROPE",
        new HashMap<AggregateType, RetUnit>(){{
          put(AggregateType.COUNT,new RetUnit(2, 0));
          put(AggregateType.SUM,new RetUnit(312855, 0));}}
    );

    resultMap.put("GERMANY|EUROPE",
        new HashMap<AggregateType, RetUnit>(){{
          put(AggregateType.COUNT,new RetUnit(1, 0));
          put(AggregateType.SUM,new RetUnit(4820, 0));}}
    );

    resultMap.put("ROMANIA|EUROPE",
        new HashMap<AggregateType, RetUnit>(){{
          put(AggregateType.COUNT,new RetUnit(2, 0));
          put(AggregateType.SUM,new RetUnit(190137, 0));}}
    );

    resultMap.put("UNITED KINGDOM|EUROPE",
        new HashMap<AggregateType, RetUnit>(){{
          put(AggregateType.COUNT,new RetUnit(1, 0));
          put(AggregateType.SUM,new RetUnit(33248, 0));}}
    );

    ArrayList<OlapRet> results = new ArrayList<>();

    for (Map.Entry<String, Map<AggregateType, RetUnit> > entry : resultMap.entrySet()) {
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

  private ArrayList<OlapRet> generateResultForEcommerce(){
    Map<String, Map<AggregateType, RetUnit> > resultMap = new HashMap<>();
    resultMap.put("2021-01",
        new HashMap<AggregateType, RetUnit>(){{
          put(AggregateType.DISTINCT,new RetUnit(235, 0));}}
    );
    ArrayList<OlapRet> results = new ArrayList<>();

    for (Map.Entry<String, Map<AggregateType, RetUnit> > entry : resultMap.entrySet()) {
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