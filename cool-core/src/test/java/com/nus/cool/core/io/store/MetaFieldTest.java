package com.nus.cool.core.io.store;

import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.io.DataOutputBuffer;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.io.readstore.MetaHashFieldRS;
import com.nus.cool.core.io.readstore.MetaRangeFieldRS;
import com.nus.cool.core.io.writestore.MetaFieldWS;
import com.nus.cool.core.io.writestore.MetaHashFieldWS;
import com.nus.cool.core.io.writestore.MetaRangeFieldWS;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * In this unit, we test MetaHashField and MetaRangeField independently.
 */
public class MetaFieldTest {

  static final Logger logger = LoggerFactory.getLogger(MetaFieldTest.class);

  private Charset charset;

  /**
   * setup.
   */
  @BeforeTest
  public void setUp() {
    logger.info("Start UnitTest " + MetaFieldTest.class.getSimpleName());
    this.charset = Charset.defaultCharset();
  }

  @AfterTest
  public void tearDown() {
    logger.info(String.format("Tear down UnitTest %s\n", MetaFieldTest.class.getSimpleName()));
  }

  @Test(dataProvider = "MetaFieldDP", enabled = false)
  public void metaFieldUnitTest(String dataDirPath) throws IOException {
    TestTable table = Utils.loadTable(dataDirPath);
    TableSchema schema = table.getSchema();
    for (FieldSchema fieldSchema : schema.getFields()) {
      if (fieldSchema.getFieldType() == FieldType.UserKey) {
        continue;
      }

      if (FieldType.isHashType(fieldSchema.getFieldType())) {
        metaHashFieldTest(table, fieldSchema.getName(), fieldSchema.getFieldType());
      } else {
        metaRangeFieldTest(table, fieldSchema.getName(), fieldSchema.getFieldType());
      }
    }
  }

  /**
   * Data provider.
   */
  @DataProvider(name = "MetaFieldDP")
  public Object[][] metaFieldDataProvider() {
    String sourcePath = Paths.get(System.getProperty("user.dir"), "src", "test", "java", "com",
        "nus", "cool", "core", "resources").toString();
    String healthPath = Paths.get(sourcePath, "health").toString();
    String tpchPath = Paths.get(sourcePath, "olap-tpch").toString();
    String sogamoPath = Paths.get(sourcePath, "sogamo").toString();
    return new Object[][] { { healthPath }, { tpchPath }, { sogamoPath }, };
  }

  /**
   * For test the logic of Test unit In product env, enanble = false.
   */
  @Test(dataProvider = "MetaHashFieldDP", enabled = false)
  public void metaHashFieldUnitTest(TestTable table, String fieldName, FieldType type)
      throws IOException {
    metaHashFieldTest(table, fieldName, type);
  }

  /**
   * Data provider.
   */
  @DataProvider(name = "MetaHashFieldDP")
  public Object[][] metaHashFieldDataProvider() {
    String sourcePath = Paths.get(System.getProperty("user.dir"), "src", "test", "java", "com",
        "nus", "cool", "core", "resources").toString();
    String healthPath = Paths.get(sourcePath, "health").toString();
    System.out.println(healthPath);
    TestTable table = Utils.loadTable(healthPath);
    System.out.println(table.toString());
    table.showTable();
    return new Object[][] { { table, "event", FieldType.Action },
        { table, "attr1", FieldType.Segment }, { table, "attr2", FieldType.Segment },
        { table, "attr3", FieldType.Segment }, };
  }

  @Test(dataProvider = "MetaRangeFieldDP", enabled = false)
  public void metaRangeFieldUnitTest(TestTable table, String fieldName, FieldType type)
      throws IOException {
    metaRangeFieldTest(table, fieldName, type);
  }

  /**
   * Data provider.
   */
  @DataProvider(name = "MetaRangeFieldDP")
  public Object[][] metaRangeFieldDataProvider() {
    String sourcePath = Paths.get(System.getProperty("user.dir"), "src", "test", "java", "com",
        "nus", "cool", "core", "resources").toString();
    String healthPath = Paths.get(sourcePath, "health").toString();
    System.out.println(healthPath);
    TestTable table = Utils.loadTable(healthPath);
    System.out.println(table.toString());
    table.showTable();
    return new Object[][] { { "birthYear", FieldType.Metric }, { "attr4", FieldType.Metric },
        { "time", FieldType.ActionTime } };
  }

  /**
   * MetaHashField Unit test.
   */
  public void metaHashFieldTest(TestTable table, String fieldName, FieldType type)
      throws IOException {
    System.out.println(fieldName + type.toString());
    int fieldIdx = table.getField2Ids().get(fieldName);
    System.out.println(fieldIdx);
    MetaFieldWS mws = new MetaHashFieldWS(type, this.charset);

    // ground-truth value
    // value : gloablId
    Map<String, Integer> res = new HashMap<>();
    int gid = 0;
    for (int idx = 0; idx < table.getRowCounts(); idx++) {
      FieldValue[] tuple = table.getTuple(idx);
      mws.put(tuple, fieldIdx);
      if (!res.containsKey(tuple[fieldIdx].getString())) {
        res.put(tuple[fieldIdx].getString(), gid++);
      }
    }

    // write
    DataOutputBuffer dob = new DataOutputBuffer();
    mws.writeTo(dob);
    // set byteBuffer
    ByteBuffer bf = ByteBuffer.wrap(dob.getData());

    // read
    MetaFieldRS mrs = new MetaHashFieldRS(this.charset);
    mrs.readFromWithFieldType(bf, type);

    Assert.assertEquals(mrs.count(), res.size());
    Assert.assertEquals(mrs.getMinValue(), 0);

    for (Map.Entry<String, Integer> entry : res.entrySet()) {
      int actual = mrs.find(entry.getKey());
      int expect = entry.getValue();
      Assert.assertEquals(actual, expect);
    }
  }

  /**
   * MetaRangeField Unit test.
   */
  public void metaRangeFieldTest(TestTable table, String fieldName, FieldType type)
      throws IOException {
    int fieldIdx = table.getField2Ids().get(fieldName);
    MetaFieldWS mws = new MetaRangeFieldWS(type);
    int max = Integer.MIN_VALUE;
    int min = Integer.MAX_VALUE;

    for (int idx = 0; idx < table.getRowCounts(); idx++) {
      FieldValue[] tuple = table.getTuple(idx);
      mws.put(tuple, fieldIdx);
      int v = tuple[fieldIdx].getInt();
      min = Math.min(min, v);
      max = Math.max(max, v);
    }

    // write
    DataOutputBuffer dob = new DataOutputBuffer();
    mws.writeTo(dob);

    ByteBuffer bf = ByteBuffer.wrap(dob.getData());

    // read
    MetaFieldRS mrs = new MetaRangeFieldRS();
    mrs.readFromWithFieldType(bf, type);

    Assert.assertEquals(mrs.getMaxValue(), max);
    Assert.assertEquals(mrs.getMinValue(), min);
  }

}
