package com.nus.cool.functionality;

import com.nus.cool.core.util.config.CsvDataLoaderConfig;
import com.nus.cool.core.util.config.DataLoaderConfig;
import com.nus.cool.model.CoolLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;

public class CsvLoaderTest {
    static final Logger logger = LoggerFactory.getLogger(CsvLoaderTest.class);

    @BeforeTest
    public void setUp() {
        logger.info("Start UnitTest " + CsvLoaderTest.class.getSimpleName());
    }

    @AfterTest
    public void tearDown() {
        logger.info(String.format("Tear down UnitTest %s\n", CsvLoaderTest.class.getSimpleName()));
    }

    @Test(priority = 1, dataProvider = "CsvLoaderTestDP")
    public void CsvLoaderUnitTest(String cube, String schemaFileName, String dataFileName, String cubeRepo)
            throws IOException {
        DataLoaderConfig config = new CsvDataLoaderConfig();
        CoolLoader loader = new CoolLoader(config);
        loader.load(cube, schemaFileName, dataFileName, cubeRepo);
    }

    @Test(dataProvider = "CsvLoaderFailTestDP", expectedExceptions = FileNotFoundException.class)
    public void CsvLoaderFailUnitTest(String cube, String schemaFileName, String dataFileName, String cubeRepo)
            throws IOException {
        DataLoaderConfig config = new CsvDataLoaderConfig();
        CoolLoader loader = new CoolLoader(config);
        loader.load(cube, schemaFileName, dataFileName, cubeRepo);
    }

//     @Test(dataProvider = "CsvLoaderConsistencyTestDP", expectedExceptions = IOException.class)
//     public void CsvLoaderConsistencyUnitTest(String cube, String schemaFileName, String dataFileName, String cubeRepo)
//             throws IOException {
//         DataLoaderConfig config = new CsvDataLoaderConfig();
//         CoolLoader loader = new CoolLoader(config);
//         loader.load(cube, schemaFileName, dataFileName, cubeRepo);
//     }

    @DataProvider(name = "CsvLoaderTestDP")
    public Object[][] CsvLoaderTestDPArgObjects() {
        return new Object[][] {
                {
                        "health",
                        Paths.get(System.getProperty("user.dir"), "..", "datasets/health", "table.yaml").toString(),
                        Paths.get(System.getProperty("user.dir"), "..", "datasets/health", "data.csv").toString(),
                        Paths.get(System.getProperty("user.dir"), "..", "CubeRepo").toString()
                },
                {
                        "sogamo",
                        Paths.get(System.getProperty("user.dir"), "..", "datasets/sogamo", "table.yaml").toString(),
                        Paths.get(System.getProperty("user.dir"), "..", "datasets/sogamo", "data.csv").toString(),
                        Paths.get(System.getProperty("user.dir"), "..", "CubeRepo").toString()
                },
                {
                        "tpc-h-10g",
                        Paths.get(System.getProperty("user.dir"), "..", "datasets/olap-tpch", "table.yaml").toString(),
                        Paths.get(System.getProperty("user.dir"), "..", "datasets/olap-tpch", "scripts", "data.csv")
                                .toString(),
                        Paths.get(System.getProperty("user.dir"), "..", "CubeRepo").toString()
                },
                {
                        "ecommerce_query",
                        Paths.get(System.getProperty("user.dir"), "..", "datasets/ecommerce_query", "table.yaml")
                                .toString(),
                        Paths.get(System.getProperty("user.dir"), "..", "datasets/ecommerce_query", "data.csv")
                                .toString(),
                        Paths.get(System.getProperty("user.dir"), "..", "CubeRepo").toString()
                },
                {
                        "health_raw",
                        Paths.get(System.getProperty("user.dir"), "..", "datasets/health_raw", "table.yaml").toString(),
                        Paths.get(System.getProperty("user.dir"), "..", "datasets/health_raw", "data.csv").toString(),
                        Paths.get(System.getProperty("user.dir"), "..", "CubeRepo").toString()
                },
                {
                        "fraud_case",
                        Paths.get(System.getProperty("user.dir"), "..", "datasets/fraud_case", "table.yaml").toString(),
                        Paths.get(System.getProperty("user.dir"), "..", "datasets/fraud_case", "data.csv").toString(),
                        Paths.get(System.getProperty("user.dir"), "..", "CubeRepo").toString()
                }
        };
    }

    @DataProvider(name = "CsvLoaderConsistencyTestDP")
    public Object[][] CsvLoaderTestDPAssertArgObjects() {
        return new Object[][] {
                {
                        "health_raw",
                        Paths.get(System.getProperty("user.dir"), "..", "datasets/health_raw", "error_table.yaml")
                                .toString(),
                        Paths.get(System.getProperty("user.dir"), "..", "datasets/health_raw", "data.csv").toString(),
                        Paths.get(System.getProperty("user.dir"), "..", "CubeRepo").toString()
                },
        };
    }

    @DataProvider(name = "CsvLoaderFailTestDP")
    public Object[][] CsvLoaderTestDPFailArgObjects() {
        return new Object[][] {
                {
                        "health",
                        Paths.get(System.getProperty("user.dir"), "..", "datasets/health", "table.yaml").toString(),
                        Paths.get(System.getProperty("user.dir"), "..", "datasets/health", "raw.csv").toString(),
                        Paths.get(System.getProperty("user.dir"), "..", "CubeRepo").toString()
                },
        };
    }
}