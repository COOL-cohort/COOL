package com.nus.cool.core.iceberg.query;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.QueryResult;
import com.nus.cool.core.iceberg.aggregator.AggregatorFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.io.FileInputStream;

import com.nus.cool.core.iceberg.result.BaseResult;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.model.CoolModel;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class IcebergQueryTest {

    IcebergQuery query;

    @BeforeMethod
    public void setUp() {
        query = new IcebergQuery();
        List<Aggregation> aggs=  new ArrayList<>();
        List<AggregatorFactory.AggregatorType> opts = new ArrayList<>();
        opts.add(AggregatorFactory.AggregatorType.COUNT);
        opts.add(AggregatorFactory.AggregatorType.SUM);
        Aggregation agg= new Aggregation();
        agg.setOperators(opts);
        agg.setFieldName("O_TOTALPRICE");
        aggs.add(agg);
        query.setAggregations(aggs);
        query.setDataSource("tpc-h-10g");
        query.setGranularity(null);
        SelectionQuery sq = new SelectionQuery();
        sq.setType(SelectionQuery.SelectionType.and);
        sq.setDimension(null);
        sq.setValues(null);
        query.setSelection(sq);
        List<String> groupFields=  new ArrayList<>();
        groupFields.add("N_NAME");
        groupFields.add("R_NAME");
        query.setGroupFields(groupFields);
        query.setTimeRange("1993-01-01|1994-01-01");
    }

    @Test
    public void ToStringTest(){
        String output= query.toString();
        System.out.println("Iceberg query print to string"+ output);
    }

    @Test (dataProvider = "IcebergQueryTestDP")
    public void ReadTest(String queryPath) throws IOException {
        String queryFilePath = queryPath; //"../olap-tpch/query.json";
        FileInputStream fin=new FileInputStream(queryFilePath);
        IcebergQuery iquery=IcebergQuery.read(fin);
        System.out.println("Read Iceberg query from json" + iquery);
    }

    @DataProvider(name = "IcebergQueryTestDP")
    public Object[][] dpArgs() {
        return new Object[][] {
                {"../datasets/olap-tpch/query.json"}
        };
    }

    @Test
    public void EcommerceDataTestSQL1() throws Exception {

        String dzFilePath = "../CubeRepo";
        String queryFilePath = "../datasets/ecommerce/queries/1.query_retention.json";

        // load query
        ObjectMapper mapper = new ObjectMapper();
        IcebergQuery query = mapper.readValue(new File(queryFilePath), IcebergQuery.class);

        // load .dz file
        String dataSourceName = query.getDataSource();
        CoolModel coolModel = new CoolModel(dzFilePath);
        coolModel.reload(dataSourceName);
        CubeRS cubers = coolModel.getCube(dataSourceName);
        List<BaseResult> results = coolModel.olapEngine.performOlapQuery(cubers, query);
        QueryResult result = QueryResult.ok(results);
        System.out.println("Result for the query is  " + result);
        coolModel.close();
    }

}