package com.nus.cool.core.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.ExtendedCohortQuery;
import com.nus.cool.core.cohort.QueryResult;
import com.nus.cool.core.cohort.funnel.FunnelQuery;
import com.nus.cool.core.iceberg.query.IcebergQuery;
import com.nus.cool.core.iceberg.result.BaseResult;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.util.config.CsvDataLoaderConfig;
import com.nus.cool.core.util.config.DataLoaderConfig;
import com.nus.cool.model.CoolOlapEngine;
import com.nus.cool.result.ExtendedResultTuple;
import com.nus.cool.model.CoolLoader;
import com.nus.cool.model.CoolModel;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class CoolModelTest {

    @Test(priority = 0)
    public static void CsvLoaderTest() throws IOException {
        System.out.println("======================== Csv data loader Test ========================");
        // System.out.println(System.getProperty("user.dir"));
        String cube = "health";
        String schemaFileName = "../health/table.yaml";
        String dataFileName = "../health/raw2.csv";
        String cubeRepo = "../datasetSource";

        DataLoaderConfig config = new CsvDataLoaderConfig();
        CoolLoader loader = new CoolLoader(config);
        loader.load(cube, schemaFileName, dataFileName, cubeRepo);

        cube = "tpc-h-10g";
        schemaFileName = "../olap-tpch/table.yaml";
        dataFileName = "../olap-tpch/scripts/data.csv";
        cubeRepo = "../datasetSource";
        loader.load(cube, schemaFileName, dataFileName, cubeRepo);

        cube = "sogamo";
        schemaFileName = "../sogamo/table.yaml";
        dataFileName = "../sogamo/test.csv";
        cubeRepo = "../datasetSource";
        loader.load(cube, schemaFileName, dataFileName, cubeRepo);
    }

    @Test (priority = 10)
    public void CubeListTest() throws IOException {
        System.out.println("======================== Cube List Test ========================");
        // System.out.println(System.getProperty("user.dir"));
        String datasetPath = "../datasetSource";
        CoolModel model = new CoolModel(datasetPath);
        String[] cubes2 = model.listCubes();
        System.out.println("Applications: " + Arrays.toString(cubes2));
    }

    @Test (priority = 1)
    public static void CubeReloadTest() throws IOException {
        System.out.println("======================== Cube Reload Test ========================");
        String datasetPath = "../datasetSource";
        String queryPath = "../health/query1-0.json";

        ObjectMapper mapper = new ObjectMapper();
        ExtendedCohortQuery query = mapper.readValue(new File(queryPath), ExtendedCohortQuery.class);

        String inputSource = query.getDataSource();
        CoolModel coolModel = new CoolModel(datasetPath);
        coolModel.reload(inputSource);
        System.out.println(coolModel);

    }

    @Test (priority = 2)
    public void CohortSelectionTest() throws IOException {
        System.out.println("======================== Cohort Selection Test ========================");
        String datasetPath = "../datasetSource";
        String queryPath = "../health/query1-0.json";

        ObjectMapper mapper = new ObjectMapper();
        ExtendedCohortQuery query = mapper.readValue(new File(queryPath), ExtendedCohortQuery.class);

        String inputSource = query.getDataSource();
        CoolModel coolModel = new CoolModel(datasetPath);
        coolModel.reload(inputSource);

        CubeRS cube = coolModel.getCube(query.getDataSource());

        List<Integer> cohorResults = coolModel.cohortEngine.selectCohortUsers(cube,null, query);
        System.out.println("Result for query is  " + cohorResults);
        List<String> userIDs = coolModel.cohortEngine.listCohortUsers(cube, cohorResults);
        System.out.println("Actual user IDs are  " + userIDs);

        String outputCohort = query.getOutputCohort();
        File cohortRoot =  new File(coolModel.getCubeStorePath(inputSource), "cohort");
        if(!cohortRoot.exists()){
            cohortRoot.mkdir();
            System.out.println("[*] Cohort Fold " + cohortRoot.getName() + " is created.");
        }
        File cohortFile = new File(cohortRoot, outputCohort);
        if (cohortFile.exists()){
            cohortFile.delete();
            System.out.println("[*] Cohort " + outputCohort + " exists and is deleted!");
        }

        coolModel.cohortEngine.createCohort(query, cohorResults, cohortRoot);
        System.out.println("[*] Cohort results are stored into " + cohortRoot.getAbsolutePath());

    }

    @Test(priority = 3)
    public void CohortAnalysis() throws IOException {
        System.out.println("======================== Cohort Analysis Test ========================");
        String datasetPath = "../datasetSource";
        String queryPath = "../health/query2.json";

        ObjectMapper mapper = new ObjectMapper();
        ExtendedCohortQuery query = mapper.readValue(new File(queryPath), ExtendedCohortQuery.class);

        String inputSource = query.getDataSource();
        CoolModel coolModel = new CoolModel(datasetPath);
        coolModel.reload(inputSource);

        if (!query.isValid())
            throw new IOException("[x] Invalid cohort query.");

        CubeRS inputCube = coolModel.getCube(query.getDataSource());
        String inputCohort = query.getInputCohort();
        if (inputCohort != null) {
            System.out.println("Input cohort: " + inputCohort);
            coolModel.loadCohorts(inputCohort, inputSource);
        }
        InputVector userVector = coolModel.getCohortUsers(inputCohort);
        List<ExtendedResultTuple> result = coolModel.cohortEngine.performCohortQuery(inputCube, userVector, query);
        System.out.println("Result for the query is  " + result);
    }

    @Test(priority = 4)
    public void FunnelAnalysis() throws IOException {
        System.out.println("======================== Funnel Analysis Test ========================");
        String datasetPath = "../datasetSource";
        String queryPath = "../sogamo/query1.json";

        ObjectMapper mapper = new ObjectMapper();
        FunnelQuery query = mapper.readValue(new File(queryPath), FunnelQuery.class);

        String inputSource = query.getDataSource();
        CoolModel coolModel = new CoolModel(datasetPath);
        coolModel.reload(inputSource);

        if (!query.isValid())
            throw new IOException("[x] Invalid cohort query.");

        CubeRS inputCube = coolModel.getCube(query.getDataSource());
        String inputCohort = query.getInputCohort();
        if (inputCohort != null) {
            System.out.println("Input cohort: " + inputCohort);
            coolModel.loadCohorts(inputCohort, inputSource);
        }
        InputVector userVector = coolModel.getCohortUsers(inputCohort);
        int[] result = coolModel.cohortEngine.performFunnelQuery(inputCube, userVector, query);
        System.out.println("Result for the query is  " + Arrays.toString(result));
    }

    @Test(priority = 5)
    public void IceBergTest() throws IOException {
        System.out.println("======================== IceBerg Test ========================");

        String dzFilePath = "../datasetSource";
        String queryFilePath = "../olap-tpch/query.json";

        // load query
        ObjectMapper mapper = new ObjectMapper();
        IcebergQuery query = mapper.readValue(new File(queryFilePath), IcebergQuery.class);

        // load .dz file
        String dataSourceName = query.getDataSource();
        CoolModel coolModel = new CoolModel(dzFilePath);
        coolModel.reload(dataSourceName);

        // execute query
        QueryResult result;
        try {
            List<BaseResult> results = coolModel.olapEngine.performOlapQuery(coolModel.getCube(dataSourceName), query);
            result = QueryResult.ok(results);
        } catch (Exception e) {
            e.printStackTrace();
            result = QueryResult.error("something wrong");
        }
        System.out.println("Result for the query is  " + result);
    }

    // @Test(priority = 6)
    public void RelationalAlgebraTest() throws Exception {
        System.out.println("======================== RelationalAlgebraTest Test ========================");

        String dzFilePath = "../datasetSource";
        String dataSourceName = "tpc-h-10g";
        // currently only support 'select'
        String operation = "select, O_ORDERPRIORITY, 2-HIGH";

        // load .dz file
        CoolModel coolModel = new CoolModel(dzFilePath);
        coolModel.reload(dataSourceName);

        IcebergQuery query = coolModel.olapEngine.generateQuery(operation, dataSourceName);
        if (query == null){
            return;
        }

        // execute query
        List<BaseResult> result = coolModel.olapEngine.performOlapQuery(coolModel.getCube(dataSourceName), query);
        System.out.println(result);
    }

    @Test(priority = 7)
    public void CohortProfilingTest() throws Exception {
        System.out.println("======================== CohortProfilingTest Test ========================");

        String dzFilePath = "../datasetSource";
        String queryFilePath = "../olap-tpch/query.json";

        // load query
        ObjectMapper mapper = new ObjectMapper();
        IcebergQuery query = mapper.readValue(new File(queryFilePath), IcebergQuery.class);

        // load .dz file
        String dataSourceName = query.getDataSource();
        CoolModel coolModel = new CoolModel(dzFilePath);
        coolModel.reload(dataSourceName);
        CubeRS cube = coolModel.getCube(dataSourceName);
        List<BaseResult> result = coolModel.olapEngine.performOlapQuery(coolModel.getCube(dataSourceName), query);
        CoolOlapEngine.profiling(result);
    }

}
