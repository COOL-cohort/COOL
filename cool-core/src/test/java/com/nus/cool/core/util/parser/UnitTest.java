package com.nus.cool.core.util.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.CohortUserSection;
import com.nus.cool.core.cohort.ExtendedCohortQuery;
import com.nus.cool.core.cohort.ExtendedCohortSelection;
import com.nus.cool.core.cohort.QueryResult;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.readstore.CubletRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.TableSchema;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.config.CsvDataLoaderConfig;
import com.nus.cool.core.util.config.DataLoaderConfig;
import com.nus.cool.loader.CoolModel;
import com.nus.cool.loader.CohortCreator;
import com.nus.cool.loader.DataLoader;
import com.nus.cool.loader.ExtendedCohortLoader;
import com.nus.cool.loader.ExtendedResultTuple;
import org.testng.annotations.Test;


public class UnitTest {
    //@Test
    public static void TableSchemaTest() {
        System.out.println(System.getProperty("user.dir"));
        try {
            File schemaFile = new File("../health/table.yaml");
            TableSchema schema = TableSchema.read(new FileInputStream(schemaFile));
            System.out.println(schema);
        } catch (IOException e){
            System.out.println(e);
            return ;
        }
    }

    //@Test
    public static void CubeLoadTest() {
        System.out.println(System.getProperty("user.dir"));
        String cube = "health";
        String schemaFileName = "../health/table.yaml";
        String cubeRepo = "../datasetSource";

        try{
            File schemaFile = new File(schemaFileName);
            File dimensionFile = new File("../health/dim2.csv");
            File dataFile = new File("../health/raw2.csv");

            TableSchema schema = TableSchema.read( new FileInputStream(schemaFile));
            Path outputCubeVersionDir = Paths.get(cubeRepo, cube, "v1");
            Files.createDirectories(outputCubeVersionDir);
            File outputDir = outputCubeVersionDir.toFile();
            DataLoaderConfig config = new CsvDataLoaderConfig();
            DataLoader loader = DataLoader.builder(cube, schema,
                    dimensionFile, dataFile, outputDir, config).build();
            loader.load();
            Files.copy(Paths.get(schemaFileName),
                    Paths.get(cubeRepo, cube, "table.yaml"),
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e){
            System.out.println(e);
            return ;
        }
    }

    @Test
    public static void CubeReloadTest() {
        String datasetPath = "../datasetSource";
        String appPath = "health";
        String queryPath = "../health/query1-0.json";

        try {
            CoolModel coolModel = new CoolModel(datasetPath);
            coolModel.reload(appPath);
            System.out.println(coolModel);
        } catch (IOException e){
            System.out.println(e);
            return ;
        }
    }

    @Test
    public static void CohortCreateTest() {
        String datasetPath = "../datasetSource";
        String appPath = "health";
        String queryPath = "../health/query1-0.json";

        try{
            ObjectMapper mapper = new ObjectMapper();
            ExtendedCohortQuery query = mapper.readValue(new File(queryPath), ExtendedCohortQuery.class);

            CoolModel coolModel = new CoolModel(datasetPath);
            coolModel.reload(appPath);

            CubeRS cube = coolModel.getCube(query.getDataSource());

            QueryResult result = CohortCreator.selectCohortUsers(cube,null, query);
            System.out.println(" result for query is  " + result.getResult());
            List<String> userIDs = CohortCreator.listCohortUsers(cube, (List<Integer>)result.getResult());
            System.out.println(" Actual user IDs are  " + userIDs);
        } catch (IOException e){
            System.out.println(e);
        }
    }

    @Test
    public static void cohortAnalysis(){
        String datasetPath = "../datasetSource";
        String queryPath = "../health/query2.json";

        try {
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
                coolModel.loadCohorts(inputCohort, datasetPath + File.separator + inputSource);
            }
            System.out.println(inputCohort);
            InputVector userVector = coolModel.getCohortUsers(inputCohort);
            QueryResult result = ExtendedCohortLoader.executeQuery(inputCube, userVector, query);
            System.out.println(" result for the query is  " + result.getResult());
        } catch (IOException e){
            System.out.println(e);
        }
    }
}
