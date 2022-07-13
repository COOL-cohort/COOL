package com.nus.cool.core.refactor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.refactor.CohortProcessor;
import com.nus.cool.core.cohort.refactor.storage.CohortRet;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.util.config.CsvDataLoaderConfig;
import com.nus.cool.core.util.config.DataLoaderConfig;
import com.nus.cool.model.CoolLoader;
import com.nus.cool.model.CoolModel;

public class ProcessorTest {
    private final String cubeRepo = "../datasetSource";
    private final String rootPath = "..";
    private final String tableName = "table.csv";
    private final String configName = "table.yaml";
    private CoolModel coolModel;

    private final String queryName = "query.json";
    private final String resultName = "query_result.json";

    @BeforeTest
    public void startUp() throws IOException{
        this.coolModel = new CoolModel(this.cubeRepo);
    }

    // @Test
    // public void ProcessorDebugTest() throws IOException {
    //     String queryPath = "../query/query_health_one/query.json";
    //     String cubeName = "health";
    //     CubeRS cube = loadData(cubeName);
    //     CohortProcessor cohortProcessor = CohortProcessor.readFromJson(queryPath);
    //     // System.out.println(cohortProcessor.getProjectedSchemaSet().toString());
    //     CohortRet ret = cohortProcessor.process(cube);
    //     System.out.println(ret.toString());
    // }


    /**
     * 
     */
    @Test(dataProvider = "ProcessQueryDP")
    public void ProcessQueryAndValidResult(String queryDir) throws IOException{
        String queryPath = Paths.get(queryDir, this.queryName).toString();
        CohortProcessor cohortProcessor = CohortProcessor.readFromJson(queryPath);
        CubeRS cube = loadData(cohortProcessor.getDataSource());
        CohortRet ret = cohortProcessor.process(cube);
        
        String queryResultPath = Paths.get(queryDir, this.resultName).toString();
        ObjectMapper mapper = new ObjectMapper();
        HashMap<String, ArrayList<Integer>> cohortData = mapper.readValue(new File(queryResultPath), HashMap.class);
       
        // check the result
        System.out.println(cohortData.toString());
        // validate the cohortName
        Assert.assertEquals(ret.getCohortList().size(), cohortData.size());

        System.out.println(ret.getCohortList());
        for(String cohortName: ret.getCohortList()){
            System.out.printf("Get CohortName %s\n", cohortName);
            List<Integer> ff = cohortData.get(cohortName);
            System.out.println(ff);
            Assert.assertTrue(cohortData.containsKey(cohortName));
            Assert.assertEquals(cohortData.get(cohortName),ret.getValuesByCohort(cohortName));
        }
    }

    @DataProvider(name = "ProcessQueryDP")
    public Object[][] queryDirDataProvider(){
        return new Object[][]{
            { new String("../query/query_health_one")}
        };
    }

    /**
     * Get the Cube from CoolModel, if the Cube is not existed
     * Load the data into CoolModel
     * @param dataSetName, source data 
     * @return
     * @throws IOException
     */
    private CubeRS loadData(String dataSetName) throws IOException{
        if(!coolModel.isCubeExsit(dataSetName)){
            DataLoaderConfig config = new CsvDataLoaderConfig();
            CoolLoader loader = new CoolLoader(config);
            String dataSetPath = Paths.get(this.rootPath, dataSetName).toString();
            String tablePath = Paths.get(dataSetPath, tableName).toString();
            String configPath = Paths.get(dataSetPath, configName).toString();
            loader.load(dataSetName, configPath, tablePath, this.cubeRepo);
        }
        coolModel.reload(dataSetName);
        return coolModel.getCube(dataSetName);
    }

}
    // @Getter
    // public class cohortResult{
    //     public HashMap<String, List<Integer>> 
    //     public cohortResult readFromJson(String path){

    //     }
    // }
