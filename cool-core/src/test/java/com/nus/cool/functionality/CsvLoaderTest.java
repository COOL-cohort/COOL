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

    @Test(dataProvider = "CsvLoaderTestDP")
    public void CsvLoaderUnitTest(String cube, String schemaFileName, String dataFileName, String cubeRepo)
            throws IOException{
        DataLoaderConfig config = new CsvDataLoaderConfig();
        CoolLoader loader = new CoolLoader(config);
        loader.load(cube, schemaFileName, dataFileName, cubeRepo);
    }

    @DataProvider(name = "CsvLoaderTestDP")
    public Object[][] csvLoaderTestDPArgObjects() {
        String sourcePath = Paths.get(System.getProperty("user.dir"),
                "src",
                "test",
                "java",
                "com",
                "nus",
                "cool",
                "core",
                "resources").toString();
        return new Object[][] {
                {
                        "health",
                        // Paths.get(sourcePath, "health", "table.yaml").toString(),
                        // Paths.get(sourcePath, "health", "table.csv").toString(),
                        Paths.get(System.getProperty("user.dir"),  "..", "health", "table.yaml").toString(),
                        Paths.get(System.getProperty("user.dir"),  "..", "health", "raw.csv").toString(),
                        Paths.get(System.getProperty("user.dir"),  "..", "datasetSource").toString()
                }, {
                        "sogamo",
                        // Paths.get(sourcePath, "sogamo", "table.yaml").toString(),
                        // Paths.get(sourcePath, "sogamo", "table.csv").toString(),
                        Paths.get(System.getProperty("user.dir"),  "..", "sogamo", "table.yaml").toString(),
                        Paths.get(System.getProperty("user.dir"),  "..", "sogamo", "test.csv").toString(),
                        Paths.get(System.getProperty("user.dir"),  "..", "datasetSource").toString()
                }, {
                        "tpc-h-10g",
                        // Paths.get(sourcePath, "olap-tpch", "table.yaml").toString(),
                        // Paths.get(sourcePath, "olap-tpch", "table.csv").toString(),
                        Paths.get(System.getProperty("user.dir"),  "..", "olap-tpch", "table.yaml").toString(),
                        Paths.get(System.getProperty("user.dir"),  "..", "olap-tpch", "scripts", "data.csv").toString(),
                        Paths.get(System.getProperty("user.dir"),  "..", "datasetSource").toString()
                },

        };
    }
}
