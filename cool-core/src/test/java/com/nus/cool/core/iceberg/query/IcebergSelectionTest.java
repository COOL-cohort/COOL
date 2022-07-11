package com.nus.cool.core.iceberg.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.model.CoolModel;

import com.nus.cool.core.io.readstore.*;
import com.nus.cool.core.schema.TableSchema;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;



public class IcebergSelectionTest {

    @Test (dataProvider = "IcebergQuerySelectionTestDP")
    public void DataChunkProceesTest(String dzPath, String queryPath) throws IOException, ParseException {
        System.out.println("======================== Process Data ChunkRS Test ========================");

        String dzFilePath = dzPath;
        String queryFilePath = queryPath;

        // load query
        ObjectMapper mapper = new ObjectMapper();
        IcebergQuery query = mapper.readValue(new File(queryFilePath), IcebergQuery.class);

        // load .dz file
        String dataSourceName = query.getDataSource();
        CoolModel coolModel = new CoolModel(dzFilePath);
        coolModel.reload(dataSourceName);

        //get data chunks
        CubeRS cube = coolModel.getCube(query.getDataSource());
        List<ChunkRS> dataChunks = cube.getCublets().get(0).getDataChunks();
        TableSchema tableSchema = cube.getSchema();
        IcebergSelection  icebergSec= new IcebergSelection();
        icebergSec.init(tableSchema,query);
        ArrayList<IcebergSelection.TimeBitSet> result= icebergSec.process(dataChunks.get(0));
        System.out.println("Iceberg Selection result=" + result);

    }

    @DataProvider(name = "IcebergQuerySelectionTestDP")
    public Object[][] dpArgs() {
        return new Object[][] {
                {"../datasetSource","../datasets/olap-tpch/query.json"}
        };
    }

}