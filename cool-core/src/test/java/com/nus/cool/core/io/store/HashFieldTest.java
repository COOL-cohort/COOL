package com.nus.cool.core.io.store;

import com.nus.cool.core.io.DataOutputBuffer;
import com.nus.cool.core.io.compression.OutputCompressor;
import com.nus.cool.core.io.readstore.DataHashFieldRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaHashFieldRS;
import com.nus.cool.core.io.writestore.DataHashFieldWS;
import com.nus.cool.core.io.writestore.MetaHashFieldWS;
import com.nus.cool.core.schema.FieldType;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * HashField UnitTest.
 */
public class HashFieldTest {
  static final Logger logger = LoggerFactory.getLogger(HashFieldTest.class);
  private String sourcePath;
  private TestTable table;
  private Charset charset;
  private OutputCompressor compressor;

  /**
   * setup.
   */
  @BeforeTest
  public void setUp() {
    logger.info("Start UnitTest " + HashFieldTest.class.getSimpleName());
    this.charset = Charset.defaultCharset();
    this.compressor = new OutputCompressor();
    sourcePath = Paths.get(System.getProperty("user.dir"), "src", "test", "java", "com", "nus",
        "cool", "core", "resources").toString();
    String filepath = Paths.get(sourcePath, "fieldtest", "table.csv").toString();
    table = TestTable.readFromCSV(filepath);
  }

  @AfterTest
  public void tearDown() {
    logger.info(String.format("Tear down UnitTest %s\n", HashFieldTest.class.getSimpleName()));
  }

  /**
   * HashFieldTest : Conversion between WriteStore and ReadStore.
   */
  @Test(dataProvider = "HashFieldTestDP")
  public void hashFieldUnitTest(String fieldName, FieldType fType) throws IOException {
    logger.info("Input HashField UnitTest Data: FieldName " + fieldName + " FieldType : "
        + fType.toString());

    int fieldidx = table.getField2Ids().get(fieldName);
    ArrayList<String> data = table.getCols().get(fieldidx);

    // Generate MetaHashFieldWS
    MetaHashFieldWS hmws = new MetaHashFieldWS(fType, charset, compressor);
    DataHashFieldWS ws = new DataHashFieldWS(hmws.getFieldType(), hmws, compressor, false);

    // Input col data into metaField
    for (int idx = 0; idx < data.size(); idx++) {
      String[] tuple = table.getTuple(idx);
      hmws.put(tuple, fieldidx);
      ws.put(data.get(idx));
    }

    // write this file into a Buffer
    DataOutputBuffer dob = new DataOutputBuffer();
    final int wsPos = hmws.writeTo(dob);
    ws.writeTo(dob);

    // Convert DataOutputBuffer to ByteBuffer
    ByteBuffer bf = ByteBuffer.wrap(dob.getData());
    bf.order(ByteOrder.nativeOrder());

    // Read from File
    MetaHashFieldRS hmrs = new MetaHashFieldRS(charset);
    hmrs.readFromWithFieldType(bf, fType);

    // validate the MetaField
    Set<String> valueSet = new HashSet<String>(data);

    for (String expected : valueSet) {
      int gid = hmrs.find(expected);
      String actual = hmrs.getString(gid);
      Assert.assertEquals(actual, expected);
    }

    bf.position(wsPos);
    FieldRS rs = DataHashFieldRS.readFrom(bf, fType);

    // Check the Key Point of HashMetaField and HashField
    Assert.assertEquals(hmws.count(), hmrs.count());

    for (int i = 0; i < data.size(); i++) {
      String expected = data.get(i);
      int globalId = rs.getValueByIndex(i);
      String actual = hmrs.getString(globalId);

      Assert.assertEquals(actual, expected);
    }
  }

  /**
   * Data provider.
   */
  @DataProvider(name = "HashFieldTestDP")
  public Object[][] dpArgs() {
    return new Object[][] {
        // { "id", FieldType.UserKey },
        { "event", FieldType.Action }, { "attr1", FieldType.Segment },
        { "attr2", FieldType.Segment }, { "attr3", FieldType.Segment }, };
  }
}
