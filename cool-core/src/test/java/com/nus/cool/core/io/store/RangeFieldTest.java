package com.nus.cool.core.io.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.ArrayList;

import com.nus.cool.core.io.DataOutputBuffer;
import com.nus.cool.core.io.compression.OutputCompressor;
import com.nus.cool.core.io.readstore.DataRangeFieldRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaRangeFieldRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.writestore.DataRangeFieldWS;
import com.nus.cool.core.io.writestore.MetaRangeFieldWS;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.util.converter.DayIntConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class RangeFieldTest {
    static final Logger logger = LoggerFactory.getLogger(RangeFieldTest.class);
    private String sourcePath;
    private TestTable table;
    private OutputCompressor compressor;

    @BeforeTest
    public void setUp() {
        logger.info("Start UnitTest " + RangeFieldTest.class.getSimpleName());
        this.compressor = new OutputCompressor();
        sourcePath = Paths.get(System.getProperty("user.dir"),
                "src",
                "test",
                "java",
                "com",
                "nus",
                "cool",
                "core",
                "resources").toString();
        String filepath = Paths.get(sourcePath, "fieldtest", "table.csv").toString();
        table = TestTable.readFromCSV(filepath);
    }

    @AfterTest
    public void tearDown() {
        logger.info(String.format("Tear down UnitTest %s\n", RangeFieldTest.class.getSimpleName()));
    }

    @Test(dataProvider = "RangeFieldTestDP")
    public void RangeFieldUnitTest(String fieldName, FieldType fType) throws IOException {
        logger.info("Input HashField UnitTest Data: FieldName " + fieldName + " FiledType : " + fType.toString());

        int fieldidx = table.getField2Ids().get(fieldName);
        ArrayList<String> data = table.getCols().get(fieldidx);
        String[] tuple = data.toArray(new String[data.size()]);
        
        // For RangeField, RangeMetaField and RangeField can be test seperatly.
        MetaRangeFieldWS rmws = new MetaRangeFieldWS(fType);
        DataRangeFieldWS ws = new DataRangeFieldWS(fType, compressor);
        // put data into writeStore
        for(int idx = 0; idx < data.size(); idx++){
            rmws.put(tuple, idx);
            ws.put(tuple[idx]);
        }

        // Write into Buffer
        DataOutputBuffer dobf = new DataOutputBuffer();
        int wsPos = rmws.writeTo(dobf);
        ws.writeTo(dobf);
        // Convert DataOutputBuffer to ByteBuffer
        ByteBuffer bf = ByteBuffer.wrap(dobf.getData());
        bf.order(ByteOrder.nativeOrder());
        // Read from Buffer
        MetaRangeFieldRS rmrs = new MetaRangeFieldRS();
      
        rmrs.readFromWithFieldType(bf, fType);
        bf.position(wsPos);
        FieldRS rs = DataRangeFieldRS.readFrom(bf, fType);

        // check Range Meta Field
        Assert.assertEquals(rmrs.getMinValue(), rmws.getMin());
        Assert.assertEquals(rmrs.getMaxValue(), rmws.getMax());
        Assert.assertEquals(rs.minKey(), rmws.getMin());
        Assert.assertEquals(rs.maxKey(), rmws.getMax());

        // check Range Vector
        InputVector vec = rs.getValueVector();
        Assert.assertEquals(vec.size(), data.size());
        DayIntConverter convertor = DayIntConverter.getInstance();
        for (int i = 0; i < vec.size(); i++) {
            String expect = data.get(i);
            if (fType == FieldType.ActionTime) {
                expect = Integer.toString(convertor.toInt(data.get(i)));
            }
            String actual = Integer.toString(rs.getValueByIndex(i));
            Assert.assertEquals(expect, actual);
        }

    }

    @DataProvider(name = "RangeFieldTestDP")
    public Object[][] rangeFieldTestDPArgObjects() {
        return new Object[][] {
                { "birthYear", FieldType.Metric },
                { "attr4", FieldType.Metric },
                { "time", FieldType.ActionTime }
        };
    }

}