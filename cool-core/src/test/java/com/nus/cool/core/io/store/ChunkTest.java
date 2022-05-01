package com.nus.cool.core.io.store;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.ArrayList;

import com.google.common.primitives.Ints;
import com.nus.cool.core.io.DataOutputBuffer;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.io.storevector.InputVector;
<<<<<<< HEAD
import com.nus.cool.core.io.writestore.DataChunkWS;
=======
import com.nus.cool.core.io.writestore.ChunkWS;
>>>>>>> 8a64ad5... Add Chunk unit Test and Generate three sample dataset with 256 items from main dataset
import com.nus.cool.core.io.writestore.MetaChunkWS;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.converter.DayIntConverter;
import com.nus.cool.core.util.parser.CsvTupleParser;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ChunkTest {

    @Test(dataProvider = "ChunkUnitTestDP")
    public void ChunkUnitTest(String dirPath) throws IOException {
        // Load Yaml Config
        String yamlFilePath = Paths.get(dirPath, "table.yaml").toString();
        File yamlFile = new File(yamlFilePath);
        TableSchema schemas = TableSchema.read(yamlFile);
        int colSize = schemas.getFields().size();

        // Load CSV file, in ArrayList
        ArrayList<String[]> data = new ArrayList<>();
        String csvFilePath = Paths.get(dirPath, "table.csv").toString();
        // File
        BufferedReader br = new BufferedReader(new FileReader(new File(csvFilePath)));
        String line;
<<<<<<< HEAD
        CsvTupleParser parser = new CsvTupleParser();
        while ((line = br.readLine()) != null) {
            String[] vec = parser.parse(line);
=======
        while ((line = br.readLine()) != null) {
            String[] vec = CsvTupleParser.Parse(line);
>>>>>>> 8a64ad5... Add Chunk unit Test and Generate three sample dataset with 256 items from main dataset
            data.add(vec);
        }

        // Generate MetaChunkWS
        MetaChunkWS metaChunkWS = MetaChunkWS.newMetaChunkWS(schemas, 0);
<<<<<<< HEAD
        DataChunkWS chunkWS = DataChunkWS.newDataChunk(schemas, metaChunkWS.getMetaFields(), 0);
=======
        ChunkWS chunkWS = ChunkWS.newChunk(schemas, metaChunkWS.getMetaFields(), 0);
>>>>>>> 8a64ad5... Add Chunk unit Test and Generate three sample dataset with 256 items from main dataset

        for (int i = 0; i < data.size(); i++) {
            // You have to update meta first,
            // you have to update globalId first
<<<<<<< HEAD
            metaChunkWS.put(data.get(i));
=======
            metaChunkWS.update(data.get(i));
>>>>>>> 8a64ad5... Add Chunk unit Test and Generate three sample dataset with 256 items from main dataset
            chunkWS.put(data.get(i));
        }

        // We create two Buffer, one for chunkWS, another for metaChunkWS
        DataOutputBuffer chunkDOB = new DataOutputBuffer();
        DataOutputBuffer metaDOB = new DataOutputBuffer();
        int chunkPOS = chunkWS.writeTo(chunkDOB);
        int metaPOS = metaChunkWS.writeTo(metaDOB);

        // ReadFrom
        ByteBuffer metaBF = ByteBuffer.wrap(metaDOB.getData());
        ByteBuffer chunkBF = ByteBuffer.wrap(chunkDOB.getData());
        metaBF.order(ByteOrder.nativeOrder());
        chunkBF.order(ByteOrder.nativeOrder());

        // decode these data
        MetaChunkRS metaChunkRS = new MetaChunkRS(schemas);
        ChunkRS chunkRS = new ChunkRS(schemas);
        // Note: we should decode the headerOffset first
        metaBF.position(metaPOS - Ints.BYTES);
        int metaHeaderOffset = metaBF.getInt();
        metaBF.position(metaHeaderOffset);
        metaChunkRS.readFrom(metaBF);
        chunkBF.position(chunkPOS - Ints.BYTES);
        int chunkHeaderOffset = chunkBF.getInt();
        chunkBF.position(chunkHeaderOffset);
        chunkRS.readFrom(chunkBF);
        ArrayList<ArrayList<String>> colTable = toColumnLayout(data);
        // Check Record Size
        Assert.assertEquals(chunkRS.getRecords(), data.size(), "Field Record Size");

        for (int i = 0; i < colSize; i++) {
            ArrayList<String> fieldValue = colTable.get(i);
            FieldSchema fschema = schemas.getField(i);
            FieldRS fieldRS = chunkRS.getField(fschema.getName());
            MetaFieldRS metaFieldRS = metaChunkRS.getMetaField(fschema.getName());
            Assert.assertTrue(isFieldCorrect(metaFieldRS, fieldRS, fieldValue),
                    String.format("Field %s Failed", fschema.getName()));
        }
    }

    @DataProvider(name = "ChunkUnitTestDP")
    public Object[][] chunkUnitTestDP() {
        String sourcePath = Paths.get(System.getProperty("user.dir"),
                "src",
                "test",
                "java",
                "com",
                "nus",
                "cool",
                "core",
                "resources").toString();
        String HealthPath = Paths.get(sourcePath, "health").toString();
        String TPCHPath = Paths.get(sourcePath, "olap-tpch").toString();
        String SogamoPath = Paths.get(sourcePath, "sogamo").toString();

        return new Object[][] {
                { HealthPath },
                { TPCHPath },
                { SogamoPath },
        };
    }

    // ------------------------------ Helper Function -----------------------------
    // //

    /**
     * The loaded data is stored in rows, transfer to column layout.
     * 
     * @param data
     * @return loaded data stored in column layout
     */
    private ArrayList<ArrayList<String>> toColumnLayout(ArrayList<String[]> data) {
        ArrayList<ArrayList<String>> res = new ArrayList<>();

        for (int i = 0; i < data.size(); i++) {
            for (int j = 0; j < data.get(0).length; j++) {

                if (i == 0) {
                    res.add(new ArrayList<String>());
                }
                res.get(j).add(data.get(i)[j]);
            }
        }

        return res;
    }

    /**
     * Check Weather the decoded Field is correct
     * 
     * @param metaFieldRS
     * @param fieldRS
     * @param fieldValue
     * @return True, correct Field or False something wrong
     */
    private Boolean isFieldCorrect(MetaFieldRS metaFieldRS, FieldRS fieldRS, ArrayList<String> fieldValue) {
        if (fieldRS.isSetField()) {
            // HashField
            InputVector local2Gloabl = fieldRS.getKeyVector();
            InputVector vec = fieldRS.getValueVector();

            for (int i = 0; i < vec.size(); i++) {
                int localID = vec.get(i);
                int globalID = local2Gloabl.get(localID);
                String actual = metaFieldRS.getString(globalID);

                if (!actual.equals(fieldValue.get(i))) {
                    return false;
                }
            }
        } else {
            // RangeField
            InputVector vec = fieldRS.getValueVector();
            DayIntConverter convertor = new DayIntConverter();
            for (int i = 0; i < vec.size(); i++) {
                String expect = fieldValue.get(i);
                if (fieldRS.getFieldType() == FieldType.ActionTime) {
                    expect = Integer.toString(convertor.toInt(expect));
                }
                String actual = Integer.toString(vec.get(i));

                if (!actual.equals(expect)) {
                    return false;
                }
            }
        }
        return true;
    }

}
