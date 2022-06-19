package com.nus.cool.functionality;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.iceberg.query.IcebergQuery;
import com.nus.cool.core.iceberg.result.AggregatorResult;
import com.nus.cool.core.iceberg.result.BaseResult;
import com.nus.cool.model.CoolModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class IcebergLoaderTest extends CsvLoaderTest {
    static final Logger logger = LoggerFactory.getLogger(IcebergLoaderTest.class);

    @BeforeTest
    public void setUp() {
        logger.info("Start UnitTest " + IcebergLoaderTest.class.getSimpleName());
    }

    @AfterTest
    public void tearDown() {
        logger.info(String.format("Tear down UnitTest %s\n", IcebergLoaderTest.class.getSimpleName()));
    }

    @Test(dataProvider = "IceBergUnitTestDP", dependsOnMethods="CsvLoaderUnitTest")
    public void IceBergTest(String datasetPath, String queryPath, List<BaseResult> out) throws Exception {
        // load query
        ObjectMapper mapper = new ObjectMapper();
        IcebergQuery query = mapper.readValue(new File(queryPath), IcebergQuery.class);

        // load .dz file
        String dataSourceName = query.getDataSource();
        CoolModel coolModel = new CoolModel(datasetPath);
        coolModel.reload(dataSourceName);

        // execute query
        List<BaseResult> results = coolModel.olapEngine.performOlapQuery(coolModel.getCube(dataSourceName), query);

        Assert.assertEquals(results, out);
    }

    @DataProvider(name = "IceBergUnitTestDP")
    public Object[][] IceBergUnitTestDPArgObjects() {
        List<BaseResult> out = new ArrayList<>();
        out.add(new BaseResult("1993-01-01|1994-01-01", "RUSSIA|EUROPE", "O_TOTALPRICE",
                new AggregatorResult(2, (long)312855, null, null, null, null, null)));
        out.add(new BaseResult("1993-01-01|1994-01-01", "GERMANY|EUROPE", "O_TOTALPRICE",
                new AggregatorResult(1, (long)4820, null, null, null, null, null)));
        out.add(new BaseResult("1993-01-01|1994-01-01", "ROMANIA|EUROPE", "O_TOTALPRICE",
                new AggregatorResult(2, (long)190137, null, null, null, null, null)));
        out.add(new BaseResult("1993-01-01|1994-01-01", "UNITED KINGDOM|EUROPE", "O_TOTALPRICE",
                new AggregatorResult(1, (long)33248, null, null, null, null, null)));

        return new Object[][] {{
                Paths.get(System.getProperty("user.dir"),  "..", "datasetSource").toString(),
                Paths.get(System.getProperty("user.dir"),  "..", "datasets/olap-tpch", "query.json").toString(),
                out
        }};
    }
}
