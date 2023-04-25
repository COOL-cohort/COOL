package com.nus.cool.core.io.store;

import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.io.DataOutputBuffer;
import com.nus.cool.core.io.readstore.DataRangeFieldRS;
import com.nus.cool.core.io.readstore.MetaRangeFieldRS;
import com.nus.cool.core.io.writestore.DataRangeFieldWS;
import com.nus.cool.core.io.writestore.MetaRangeFieldWS;
import com.nus.cool.core.schema.FieldType;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * RangeField Unit test.
 */
public class RangeFieldTest {
  static final Logger logger = LoggerFactory.getLogger(RangeFieldTest.class);
  private String sourcePath;
  private TestTable table;

  /**
   * setup.
   */
  @BeforeTest
  public void setUp() {
    logger.info("Start UnitTest " + RangeFieldTest.class.getSimpleName());
    sourcePath = Paths.get(System.getProperty("user.dir"), "src", "test", "java", "com", "nus",
        "cool", "core", "resources").toString();
    String filepath = Paths.get(sourcePath, "fieldtest").toString();
    table = Utils.loadTable(filepath);
  }

  @AfterTest
  public void tearDown() {
    logger.info(String.format("Tear down UnitTest %s\n", RangeFieldTest.class.getSimpleName()));
  }

  @Test(dataProvider = "RangeFieldTestDP")
  public void rangeFieldUnitTest(String fieldName, FieldType fType) throws IOException {
    logger.info("Input HashField UnitTest Data: FieldName " + fieldName + " FieldType : "
        + fType.toString());

    int fieldidx = table.getField2Ids().get(fieldName);
    ArrayList<FieldValue> data = table.getCols().get(fieldidx);
    FieldValue[] tuple = data.toArray(new FieldValue[data.size()]);

    // For RangeField, RangeMetaField and RangeField can be test seperatly.
    MetaRangeFieldWS rmws = new MetaRangeFieldWS(fType);
    DataRangeFieldWS ws = new DataRangeFieldWS(fType);
    // put data into writeStore
    for (int idx = 0; idx < data.size(); idx++) {
      rmws.put(tuple, idx);
      ws.put(tuple[idx]);
    }

    // Write into Buffer
    DataOutputBuffer dobf = new DataOutputBuffer();
    int wsPos = rmws.writeTo(dobf);
    ws.writeTo(dobf);
    // Convert DataOutputBuffer to ByteBuffer
    ByteBuffer bf = ByteBuffer.wrap(dobf.getData());
    // Read from Buffer
    MetaRangeFieldRS rmrs = new MetaRangeFieldRS();

    rmrs.readFromWithFieldType(bf, fType);
    bf.position(wsPos);
    DataRangeFieldRS rs = DataRangeFieldRS.readFrom(bf, fType);

    // check Range Meta Field
    Assert.assertEquals(rmrs.getMinValue(), rmws.getMin());
    Assert.assertEquals(rmrs.getMaxValue(), rmws.getMax());
    Assert.assertEquals(rs.minKey(), rmws.getMin());
    Assert.assertEquals(rs.maxKey(), rmws.getMax());

    // check Range Vector
    for (int i = 0; i < data.size(); i++) {
      String expect = data.get(i).getString();
      String actual = rs.getValueByIndex(i).getString();
      Assert.assertEquals(expect, actual);
    }

  }

  @DataProvider(name = "RangeFieldTestDP")
  public Object[][] rangeFieldTestDPArgObjects() {
    return new Object[][] { { "birthYear", FieldType.Metric }, { "attr4", FieldType.Metric },
        { "time", FieldType.ActionTime } };
  }
}
