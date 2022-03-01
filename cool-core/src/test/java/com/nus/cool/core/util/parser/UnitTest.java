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
import com.nus.cool.loader.DataLoader;



public class UnitTest {
    public static void TableSchemaTest() throws IOException {
        System.out.println(System.getProperty("user.dir"));
        File schemaFile = new File("health/table.yaml");
        TableSchema schema = TableSchema.read( new FileInputStream(schemaFile));
        System.out.println(schema);
    }

    public static void CubeReloadTest() throws IOException {
        String datasetPath = "datasetSource";
        String appPath = "health";
        String queryPath = "health/query1-0.json";

        CoolModel coolModel = new CoolModel(datasetPath);
        coolModel.reload(appPath);
        System.out.println(coolModel);
    }

    public static void CohortCreateTest() throws IOException {
        String datasetPath = "datasetSource";
        String appPath = "health";
        String queryPath = "health/query1-0.json";

        ObjectMapper mapper = new ObjectMapper();
        ExtendedCohortQuery query = mapper.readValue(new File(queryPath), ExtendedCohortQuery.class);

        CoolModel coolModel = new CoolModel(datasetPath);
        coolModel.reload(appPath);

        QueryResult result = selectCohortUsers(coolModel.getCube(query.getDataSource()),null, query);
        System.out.println(" result for query0 is  " + result);
    }

    public static QueryResult selectCohortUsers(CubeRS cube,
                                                InputVector users,
                                                ExtendedCohortQuery query) throws IOException {
        if (cube == null)
            throw new IOException("data source is null");

        List<CubletRS> cublets = cube.getCublets();
        TableSchema tableSchema = cube.getSchema();
        List<Integer> userList = new ArrayList<>();

        for (CubletRS cubletRS : cublets) {
            MetaChunkRS metaChunk = cubletRS.getMetaChunk();
            ExtendedCohortSelection sigma = new ExtendedCohortSelection();
            CohortUserSection gamma = new CohortUserSection(sigma);
            gamma.init(tableSchema, users, query);
            gamma.process(metaChunk);
            if(sigma.isUserActiveCublet()) {
                List<ChunkRS> dataChunks = cubletRS.getDataChunks();
                for(ChunkRS dataChunk : dataChunks) {
                    gamma.process(dataChunk);
                }
            }

            userList.addAll((List<Integer>)gamma.getCubletResults());
        }

        return QueryResult.ok(userList);
    }

    public static void CubeLoadTest() throws IOException{
        System.out.println(System.getProperty("user.dir"));
        String cube = "health";
        String schemaFileName = "health/table.yaml";
        File schemaFile = new File(schemaFileName);
        File dimensionFile = new File("health/dim2.csv");
        File dataFile = new File("health/raw2.csv");
        String cubeRepo = "./datasetSource";

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
    }

    public static void main(String[] args) throws IOException {
        CubeLoadTest();
    }
}
