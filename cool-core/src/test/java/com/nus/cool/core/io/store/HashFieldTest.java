package com.nus.cool.core.io.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;

import com.nus.cool.core.io.DataOutputBuffer;
import com.nus.cool.core.io.compression.OutputCompressor;
import com.nus.cool.core.io.readstore.CoolFieldRS;
import com.nus.cool.core.io.readstore.HashMetaFieldRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.writestore.DataHashFieldWS;
import com.nus.cool.core.io.writestore.MetaHashFieldWS;
import com.nus.cool.core.schema.FieldType;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * HashField UnitTest
 * 
 * @author lingze
 */
public class HashFieldTest {
    static final Logger logger = LoggerFactory.getLogger(HashFieldTest.class);
    private String sourcePath;
    private TestTable table;
    private Charset charset;
    private OutputCompressor compressor;

    @BeforeTest
    public void setUp() {
        logger.info("Start UnitTest " + HashFieldTest.class.getSimpleName());
        this.charset = Charset.defaultCharset();
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
        logger.info(String.format("Pass UnitTest %s\n", HashFieldTest.class.getSimpleName()));
    }

    /**
     * HashFiledTest : Conversion between WriteStore and ReadStore
     * 
     * @param fieldName
     * @param fType
     * @throws IOException
     */
    @Test(dataProvider = "HashFieldTestDP")
    public void HashFieldUnitTest(String fieldName, FieldType fType) throws IOException {
        logger.info("Input HashField UnitTest Data: FieldName " + fieldName + " FiledType : " + fType.toString());

        int fieldidx = table.field2Ids.get(fieldName);
        ArrayList<String> data = table.cols.get(fieldidx);

        // Generate MetaHashFieldWS
        MetaHashFieldWS hmws = new MetaHashFieldWS(fType, charset, compressor);
        // Input col data into metaField
        for (String value : data) {
            hmws.put(value);
        }
        DataHashFieldWS ws = new DataHashFieldWS(hmws.getFieldType(), fieldidx, hmws, compressor, false);
        // Input col data into Field
        for (String value : data) {
            ws.put(value);
        }

        // write this file into a Buffer
        DataOutputBuffer dob = new DataOutputBuffer();
        int wsPos = hmws.writeTo(dob);
        ws.writeTo(dob);

        // Convert DataOutputBuffer to ByteBuffer
        ByteBuffer bf = ByteBuffer.wrap(dob.getData());
        bf.order(ByteOrder.nativeOrder());

        // Read from File
        HashMetaFieldRS hmrs = new HashMetaFieldRS(charset);
        hmrs.readFromWithFieldType(bf, fType);
        bf.position(wsPos);
        CoolFieldRS rs = new CoolFieldRS();
        rs.readFromWithFieldType(bf, fType);

        // Check the Key Point of HashMetaField and HashField
        Assert.assertEquals(hmws.count(), hmrs.count());

        // localId 2 GlobalId
        InputVector lid2Gid = rs.getKeyVector();
        // Tuple global Id
        InputVector vec = rs.getValueVector();
        for (int i = 0; i < vec.size(); i++) {
            String expected = data.get(i);
            int localId = vec.get(i);
            int globalId = lid2Gid.get(localId);
            String actual = hmrs.getString(globalId);

            Assert.assertEquals(actual, expected);
        }
    }

    @DataProvider(name = "HashFieldTestDP")
    public Object[][] dpArgs() {
        return new Object[][] {
                { "id", FieldType.UserKey },
                { "event", FieldType.Action },
                { "attr1", FieldType.Segment },
                { "attr2", FieldType.Segment },
                { "attr3", FieldType.Segment },
        };
    }
}
