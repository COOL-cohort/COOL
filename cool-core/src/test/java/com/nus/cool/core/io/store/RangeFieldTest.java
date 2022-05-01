package com.nus.cool.core.io.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.ArrayList;

import com.nus.cool.core.io.DataOutputBuffer;
import com.nus.cool.core.io.compression.OutputCompressor;
import com.nus.cool.core.io.readstore.CoolFieldRS;
import com.nus.cool.core.io.readstore.RangeMetaFieldRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.writestore.DataRangeFieldWS;
import com.nus.cool.core.io.writestore.MetaRangeFieldWS;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.util.converter.DayIntConverter;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class RangeFieldTest {
    private String sourcePath;
    private TestTable table;
    private OutputCompressor compressor;

    @BeforeTest
    public void setUp() {
        System.out.println("RangeField Test SetUp");
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

    @Test(dataProvider = "RangeFieldTestDP")
    public void RangeFieldUnitTest(String fieldName, FieldType fType) throws IOException {
        System.out.printf("RangeFieldTest UnitInput: FieldName %s\tFieldType %s\n", fieldName, fType);
        int fieldidx = table.field2Ids.get(fieldName);
        ArrayList<String> data = table.cols.get(fieldidx);

        // For RangeField, RangeMetaField and RangeField can be test seperatly.
        MetaRangeFieldWS rmws = new MetaRangeFieldWS(fType);
        DataRangeFieldWS ws = new DataRangeFieldWS(fType, 0, compressor);
        // put data into writeStore
        for (String v : data) {
            rmws.put(v);
            ws.put(v);
        }
        // Write into Buffer
        DataOutputBuffer dobf = new DataOutputBuffer();
        int wsPos = rmws.writeTo(dobf);
        ws.writeTo(dobf);
        // Convert DataOutputBuffer to ByteBuffer
        ByteBuffer bf = ByteBuffer.wrap(dobf.getData());
        bf.order(ByteOrder.nativeOrder());
        // Read from Buffer
        RangeMetaFieldRS rmrs = new RangeMetaFieldRS();
        CoolFieldRS rs = new CoolFieldRS();
        rmrs.readFromWithFieldType(bf, fType);
        bf.position(wsPos);
        rs.readFromWithFieldType(bf, fType);

        // check Range Meta Field
        Assert.assertEquals(rmrs.getMinValue(), rmws.getMin());
        Assert.assertEquals(rmrs.getMaxValue(), rmws.getMax());
        Assert.assertEquals(rs.minKey(), rmws.getMin());
        Assert.assertEquals(rs.maxKey(), rmws.getMax());

        // check Range Vector
        InputVector vec = rs.getValueVector();
        Assert.assertEquals(vec.size(), data.size());
        DayIntConverter convertor = new DayIntConverter();
        for (int i = 0; i < vec.size(); i++) {
            String expect = data.get(i);
            if (fType == FieldType.ActionTime) {
                expect = Integer.toString(convertor.toInt(data.get(i)));
            }
            String actual = Integer.toString(vec.get(i));
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
