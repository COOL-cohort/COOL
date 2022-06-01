
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

    // @Test(priority = 6)
    public void RelationalAlgebraTest() throws Exception {
        System.out.println("======================== Relational AlgebraTest Test ========================");

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

    // @Test(priority = 7)
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
