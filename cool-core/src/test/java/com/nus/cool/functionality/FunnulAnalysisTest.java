package com.nus.cool.functionality;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.funnel.FunnelQuery;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.model.CoolModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

public class FunnulAnalysisTest extends CsvLoaderTest{
    static final Logger logger = LoggerFactory.getLogger(FunnulAnalysisTest.class);

    @BeforeTest
    public void setUp() {
        logger.info("Start UnitTest " + FunnulAnalysisTest.class.getSimpleName());
    }

    @AfterTest
    public void tearDown() {
        logger.info(String.format("Tear down UnitTest %s\n", FunnulAnalysisTest.class.getSimpleName()));
    }

    @Test(dataProvider = "FunnelAnalysisTestDP", dependsOnMethods="CsvLoaderUnitTest")
    public void FunnelAnalysisUnitTest(String datasetPath, String queryPath, int[] out) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        FunnelQuery query = mapper.readValue(new File(queryPath), FunnelQuery.class);

        String inputSource = query.getDataSource();
        CoolModel coolModel = new CoolModel(datasetPath);
        coolModel.reload(inputSource);

        Assert.assertTrue(query.isValid());

        CubeRS inputCube = coolModel.getCube(query.getDataSource());
        String inputCohort = query.getInputCohort();
        if (inputCohort != null) {
            coolModel.loadCohorts(inputCohort, inputSource);
        }
        InputVector userVector = coolModel.getCohortUsers(inputCohort);
        int[] result = coolModel.cohortEngine.performFunnelQuery(inputCube, userVector, query);

        Assert.assertEquals(result, out);
    }

    @DataProvider(name = "FunnelAnalysisTestDP")
    public Object[][] FunnelAnalysisTestDPArgObjects() {
        int[] out = {5, 5, 4, 4};
        return new Object[][] {{
                Paths.get(System.getProperty("user.dir"),  "..", "CubeRepo").toString(),
                Paths.get(System.getProperty("user.dir"),  "..", "datasets/sogamo", "query1.json").toString(),
                out
        }};
    }
}
