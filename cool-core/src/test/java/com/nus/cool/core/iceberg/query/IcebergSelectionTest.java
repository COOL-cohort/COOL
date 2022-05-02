package com.nus.cool.core.iceberg.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.model.CoolModel;

import com.nus.cool.core.io.readstore.*;
import com.nus.cool.core.schema.TableSchema;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;



public class IcebergSelectionTest {

    @Test
    public void DataChunkProceesTest() throws IOException, ParseException {
        System.out.println("======================== Process Data ChunkRS Test ========================");

        String dzFilePath = "../datasetSource";
        String queryFilePath = "../olap-tpch/query.json";

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
        Map<String, BitSet> result= icebergSec.process(dataChunks.get(0));
        System.out.println("Iceberg Selection result=" + result);

    }

}