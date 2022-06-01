
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
        String dataFileName = "../health/raw.csv";
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
        String[] cubes2 = CoolModel.listCubes(datasetPath);
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

    @Test(priority = 5)
    public void IceBergTest() throws Exception {
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
        List<BaseResult> results = coolModel.olapEngine.performOlapQuery(coolModel.getCube(dataSourceName), query);
//        assert results.get(0).getAggregatorResult().getCount().equals((float)2.0);
//        assert results.get(0).getAggregatorResult().getSum().equals((long)312855);
//
//        assert results.get(1).getAggregatorResult().getCount().equals((float)1.0);
//        assert results.get(1).getAggregatorResult().getSum().equals((long)4820);
//
//        assert results.get(2).getAggregatorResult().getCount().equals((float)2.0);
//        assert results.get(2).getAggregatorResult().getSum().equals((long)190137);
//
//        assert results.get(3).getAggregatorResult().getCount().equals((float)1);
//        assert results.get(3).getAggregatorResult().getSum().equals((long)33248);
        QueryResult result = QueryResult.ok(results);
        System.out.println("Result for the query is  " + result);
    }

    // @Test(priority = 6)
    public void RelationalAlgebraTest() throws Exception {
        System.out.println("======================== RelationalAlgebraTest Test ========================");

        String dzFilePath = "../datasetSource";
        String dataSourceName = "tpc-h-10g";
        // currently, only support 'select'
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
