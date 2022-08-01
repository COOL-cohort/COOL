package com.nus.cool.core.refactor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.refactor.CohortProcessor;
import com.nus.cool.core.cohort.refactor.storage.CohortRet;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.model.CoolModel;

public class ProcessorTest {
    static final Logger logger = LoggerFactory.getLogger(ProcessorTest.class);
    private final String cubeRepo = "../CubeRepo";
    private CoolModel coolModel;

    private final String queryName = "query.json";
    private final String resultName = "query_result.json";

    @BeforeTest
    public void setUp(){
        logger.info("Start UnitTest " + ProcessorTest.class.getSimpleName());
    }

    public void tearDown() {
        logger.info(String.format("Tear down UnitTest %s\n", ProcessorTest.class.getSimpleName()));
    }

    // Display the cohort result
    // @Test(dataProvider = "ProcessQueryDP")
    // public void ProcessorDebugTest(String queryDir) throws IOException {
    //     String queryPath = Paths.get(queryDir, this.queryName).toString();
    //     CohortProcessor cohortProcessor = CohortProcessor.readFromJson(queryPath);
    //     CubeRS cube = loadData(cohortProcessor.getDataSource());
    //     CohortRet ret = cohortProcessor.process(cube);
    //     System.out.printf("Cohort List : %s\n", ret.getCohortList().toString());
    //     System.out.println(ret.toString());
    // }

    /**
     * 
     */
    @Test(dataProvider = "ProcessQueryDP")
    public void ProcessQueryAndValidResult(String queryDir) throws IOException{
        String queryPath = Paths.get(queryDir, this.queryName).toString();
        CohortProcessor cohortProcessor = CohortProcessor.readFromJson(queryPath);

        // start a new cool model and reload the cube
        this.coolModel = new CoolModel(this.cubeRepo);
        coolModel.reload(cohortProcessor.getDataSource());
        CubeRS cube = coolModel.getCube(cohortProcessor.getDataSource());

        CohortRet ret = cohortProcessor.process(cube);
        
        String queryResultPath = Paths.get(queryDir, this.resultName).toString();
        ObjectMapper mapper = new ObjectMapper();
        HashMap<String, ArrayList<Integer>> cohortData = mapper.readValue(new File(queryResultPath), HashMap.class);
        // check the result
        System.out.println(ret.toString());
        // validate the cohortName
        Assert.assertEquals(ret.getCohortList().size(), cohortData.size());
        
        System.out.println(ret.getCohortList());
        for(String cohortName: ret.getCohortList()){
            System.out.printf("Get CohortName %s\n", cohortName);
            Assert.assertTrue(cohortData.containsKey(cohortName));
            System.out.printf("True Result %s\n", cohortData.get(cohortName).toString());
            System.out.printf("Get Result %s\n", ret.getValuesByCohort(cohortName));
            Assert.assertEquals(cohortData.get(cohortName),ret.getValuesByCohort(cohortName));
        }
    }

    @DataProvider(name = "ProcessQueryDP")
    public Object[][] queryDirDataProvider(){
        return new Object[][]{
            { new String("../query/query_0")},
            { new String("../query/query_1")},
        };
    }

}

