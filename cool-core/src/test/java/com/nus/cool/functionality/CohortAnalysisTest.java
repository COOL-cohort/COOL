package com.nus.cool.functionality;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.ExtendedCohortQuery;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.model.CoolModel;
import com.nus.cool.result.ExtendedResultTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class CohortAnalysisTest extends CohortSelectionTest {
    static final Logger logger = LoggerFactory.getLogger(CohortAnalysisTest.class);

    @BeforeTest
    public void setUp() {
        logger.info("Start UnitTest " + CohortAnalysisTest.class.getSimpleName());
    }

    @AfterTest
    public void tearDown() {
        logger.info(String.format("Tear Down UnitTest %s\n", CohortAnalysisTest.class.getSimpleName()));
    }

    @Test(dataProvider = "CohortAnalysisTestDP", dependsOnMethods="CohortSelectionUnitTest")
    public void CohortAnalysisUnitTest(String datasetPath, String queryPath, List<ExtendedResultTuple> out) throws IOException {
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
            coolModel.loadCohorts(inputCohort, inputSource);
        }
        InputVector userVector = coolModel.getCohortUsers(inputCohort);
        List<ExtendedResultTuple> result = coolModel.cohortEngine.performCohortQuery(inputCube, userVector, query);
        logger.info(""+result);
        assert result.equals(out);
    }

    @DataProvider(name = "CohortAnalysisTestDP")
    public Object[][] ArgObjects() {
        List<ExtendedResultTuple> out1 = new ArrayList<>();
        out1.add(new ExtendedResultTuple("((1970, 1980])", 0, 3.0, 22.0, 22.0, 22.0, 1.0));
        out1.add(new ExtendedResultTuple("((1970, 1980])", 1, 1.0, 31.0, 31.0, 31.0, 1.0));
        out1.add(new ExtendedResultTuple("((1970, 1980])", 2, 1.0, 37.0, 37.0, 37.0, 1.0));
        out1.add(new ExtendedResultTuple("((1970, 1980])", 3, 1.0, 1.0, 1.0, 1.0, 1.0));
        out1.add(new ExtendedResultTuple("((1980, 1990])", 0, 2.0, 0.0, 0.0, 0.0, 0.0));
        out1.add(new ExtendedResultTuple("((1990, 2000))", 0, 2.0, 29.0, 29.0, 29.0, 1.0));
        out1.add(new ExtendedResultTuple("((1990, 2000))", 1, 1.0, 55.0, 55.0, 55.0, 1.0));
        out1.add(new ExtendedResultTuple("((1990, 2000))", 2, 1.0, 10.0, 10.0, 10.0, 1.0));

        List<ExtendedResultTuple> out2 = new ArrayList<>();
        out2.add(new ExtendedResultTuple("((1970, 1980])", 0, 1.0, 0.0, 0.0, 0.0, 0.0));
        out2.add(new ExtendedResultTuple("((1980, 1990])", 0, 1.0, 0.0, 0.0, 0.0, 0.0));
        out2.add(new ExtendedResultTuple("((1990, 2000))", 0, 1.0, 0.0, 0.0, 0.0, 0.0));
        out2.add(new ExtendedResultTuple("((1990, 2000))", 1, 1.0, 55.0, 55.0, 55.0, 1.0));

        return new Object[][] {
                {
                        Paths.get(System.getProperty("user.dir"),  "..", "datasetSource").toString(),
                        Paths.get(System.getProperty("user.dir"),  "..", "health", "query2.json").toString(),
                        out1
                },{
                        Paths.get(System.getProperty("user.dir"),  "..", "datasetSource").toString(),
                        Paths.get(System.getProperty("user.dir"),  "..", "health", "query1-1.json").toString(),
                        out2
                }
        };
    }

}