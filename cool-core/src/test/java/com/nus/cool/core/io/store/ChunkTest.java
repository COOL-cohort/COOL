package com.nus.cool.core.io.store;

import com.google.common.primitives.Ints;
import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.io.DataOutputBuffer;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.io.writestore.DataChunkWS;
import com.nus.cool.core.io.writestore.MetaChunkWS;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
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
 * Testing data chunk read store and write store.
 */
public class ChunkTest {

  static final Logger logger = LoggerFactory.getLogger(ChunkTest.class);

  @BeforeTest
  public void setUp() {
    logger.info("Start UnitTest " + ChunkTest.class.getSimpleName());
  }

  @AfterTest
  public void tearDown() {
    logger.info(String.format("Tear down UnitTest %s\n", ChunkTest.class.getSimpleName()));
  }

  @Test(dataProvider = "ChunkUnitTestDP")
  public void chunkUnitTest(String dirPath) throws IOException {
    logger.info("Input Chunk UnitTest Data: DataPath " + dirPath);

    TestTable table = Utils.loadTable(dirPath);
    TableSchema schemas = table.getSchema();

    // Generate MetaChunkWS
    MetaChunkWS metaChunkWS = MetaChunkWS.newMetaChunkWS(schemas, 0);
    DataChunkWS chunkWS = DataChunkWS.newDataChunk(schemas, metaChunkWS.getMetaFields(), 0);

    for (int i = 0; i < table.getRowCounts(); i++) {
      // You have to update meta first,
      // you have to update globalId first
      FieldValue[] tuple = table.getTuple(i);
      metaChunkWS.put(tuple);
      chunkWS.put(tuple);
    }

    // We create two Buffer, one for chunkWS, another for metaChunkWS
    DataOutputBuffer chunkDOB = new DataOutputBuffer();
    DataOutputBuffer metaDOB = new DataOutputBuffer();
    final int chunkPOS = chunkWS.writeTo(chunkDOB);
    final int metaPOS = metaChunkWS.writeTo(metaDOB);

    // ReadFrom
    ByteBuffer metaBF = ByteBuffer.wrap(metaDOB.getData());
    ByteBuffer chunkBF = ByteBuffer.wrap(chunkDOB.getData());

    // decode these data
    MetaChunkRS metaChunkRS = new MetaChunkRS(schemas);
    ChunkRS chunkRS = new ChunkRS(schemas, metaChunkRS);
    // Note: we should decode the headerOffset first
    metaBF.position(metaPOS - Ints.BYTES);
    int metaHeaderOffset = metaBF.getInt();
    metaBF.position(metaHeaderOffset);
    metaChunkRS.readFrom(metaBF);
    chunkBF.position(chunkPOS - Ints.BYTES);
    int chunkHeaderOffset = chunkBF.getInt();
    chunkBF.position(chunkHeaderOffset);
    chunkRS.readFrom(chunkBF);

    // Check Record Size
    Assert.assertEquals(chunkRS.getRecords(), table.getRowCounts(), "Field Record Size");

    for (int i = 0; i < table.getColCounts(); i++) {
      ArrayList<FieldValue> fieldValue = table.getCols().get(i);
      FieldSchema fschema = schemas.getField(i);
      FieldRS fieldRS = chunkRS.getField(fschema.getName());
      MetaFieldRS metaFieldRS = metaChunkRS.getMetaField(fschema.getName());
      Assert.assertTrue(isFieldCorrect(metaFieldRS, fieldRS, fieldValue),
          String.format("Field %s Failed", fschema.getName()));
    }
  }

  /**
   * Data provider.
   */
  @DataProvider(name = "ChunkUnitTestDP")
  public Object[][] chunkUnitTestDP() {
    String sourcePath = Paths.get(System.getProperty("user.dir"), "src", "test", "java", "com",
        "nus", "cool", "core", "resources").toString();
    String healthPath = Paths.get(sourcePath, "health").toString();
    String tpchPath = Paths.get(sourcePath, "olap-tpch").toString();
    String sogamoPath = Paths.get(sourcePath, "sogamo").toString();

    return new Object[][] { { healthPath }, { tpchPath }, { sogamoPath }, };
  }

  // ------------------------------ Helper Function -----------------------------

  /**
   * Check Weather the decoded Field is correct.
   *
   * @return True, correct Field or False something wrong
   */
  private Boolean isFieldCorrect(MetaFieldRS metaFieldRS, FieldRS fieldRS,
      ArrayList<FieldValue> valueList) {
    if (FieldType.isHashType(fieldRS.getFieldType())) {
      // HashField
      for (int i = 0; i < valueList.size(); i++) {
        int gid = fieldRS.getValueByIndex(i).getInt();
        String actual = metaFieldRS.get(gid).map(FieldValue::getString).orElse("");
        if (!actual.equals(valueList.get(i).getString())) {
          return false;
        }
      }
    } else {
      // RangeField
      for (int i = 0; i < valueList.size(); i++) {
        String expect = valueList.get(i).getString();
        String actual = fieldRS.getValueByIndex(i).getString();
        if (!actual.equals(expect)) {
          return false;
        }
      }
    }
    return true;
  }

}
